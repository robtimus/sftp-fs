/*
 * SSHChannelPool.java
 * Copyright 2016 Rob Spoor
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.robtimus.filesystems.sftp;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import com.github.robtimus.pool.Pool;
import com.github.robtimus.pool.PoolConfig;
import com.github.robtimus.pool.PoolLogger;
import com.github.robtimus.pool.PoolableObject;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpStatVFS;

/**
 * A pool of SSH channels, allowing multiple commands to be executed concurrently.
 *
 * @author Rob Spoor
 */
final class SSHChannelPool {

    private final JSch jsch;

    private final String hostname;
    private final int port;

    private final SFTPEnvironment env;
    private final FileSystemExceptionFactory exceptionFactory;

    private final Pool<Channel, IOException> pool;

    SSHChannelPool(String hostname, int port, SFTPEnvironment env) throws IOException {
        jsch = env.createJSch();

        this.hostname = hostname;
        this.port = port;
        this.env = env;
        this.exceptionFactory = env.getExceptionFactory();

        PoolConfig config = env.getPoolConfig().config();
        PoolLogger logger = PoolLogger.custom()
                .withLoggerClass(SSHChannelPool.class)
                .withMessagePrefix((port == -1 ? hostname : hostname + ":" + port) + " - ") //$NON-NLS-1$ //$NON-NLS-2$
                .withObjectPrefix("channel-") //$NON-NLS-1$
                .build();
        pool = new Pool<>(config, Channel::new, logger);
    }

    Channel get() throws IOException {
        try {
            return pool.acquire(() -> new IOException(SFTPMessages.clientConnectionWaitTimeoutExpired()));

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            InterruptedIOException iioe = new InterruptedIOException(e.getMessage());
            iioe.initCause(e);
            throw iioe;
        }
    }

    Channel getOrCreate() throws IOException {
        return pool.acquireOrCreate();
    }

    void keepAlive() throws IOException {
        // Actually, no need to do anything; channels are validated using a keep-alive signal by the forAllIdleObjects call
        pool.forAllIdleObjects(channel -> {
            // does nothing
        });
    }

    void close() throws IOException {
        pool.shutdown();
    }

    final class Channel extends PoolableObject<IOException> implements Closeable {

        private final ChannelSftp channelSftp;

        private Channel() throws IOException {
            channelSftp = env.openChannel(jsch, hostname, port);
        }

        @Override
        protected boolean validate() {
            if (channelSftp.isConnected()) {
                try {
                    channelSftp.getSession().sendKeepAliveMsg();
                    return true;
                } catch (@SuppressWarnings("unused") Exception e) {
                    // the keep alive failed - let the pool call releaseResources
                }
            }
            return false;
        }

        @Override
        protected void releaseResources() throws IOException {
            channelSftp.disconnect();
            try {
                channelSftp.getSession().disconnect();
            } catch (JSchException e) {
                throw asIOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            release();
        }

        String pwd() throws IOException {
            try {
                return channelSftp.pwd();
            } catch (SftpException e) {
                throw asIOException(e);
            }
        }

        InputStream newInputStream(String path, OpenOptions options) throws IOException {
            assert options.read;

            try {
                InputStream in = channelSftp.get(path);
                in = new SFTPInputStream(path, in, options.deleteOnClose);
                addReference(in);
                return in;
            } catch (SftpException e) {
                throw exceptionFactory.createNewInputStreamException(path, e);
            }
        }

        private final class SFTPInputStream extends InputStream {

            private final String path;
            private final InputStream in;
            private final boolean deleteOnClose;

            private boolean open = true;

            private SFTPInputStream(String path, InputStream in, boolean deleteOnClose) {
                this.path = path;
                this.in = in;
                this.deleteOnClose = deleteOnClose;
                logEvent(() -> SFTPMessages.log.createdInputStream(path));
            }

            @Override
            public int read() throws IOException {
                return in.read();
            }

            @Override
            public int read(byte[] b) throws IOException {
                return in.read(b);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return in.read(b, off, len);
            }

            @Override
            public long skip(long n) throws IOException {
                return in.skip(n);
            }

            @Override
            public int available() throws IOException {
                return in.available();
            }

            @Override
            public void close() throws IOException {
                if (open) {
                    try {
                        in.close();
                    } finally {
                        // always finalize the stream, to prevent pool starvation
                        // set open to false as well, to prevent finalizing the stream twice
                        open = false;
                        removeReference(this);
                    }
                    if (deleteOnClose) {
                        delete(path, false);
                    }
                    logEvent(() -> SFTPMessages.log.closedInputStream(path));
                }
            }

            @Override
            public synchronized void mark(int readlimit) {
                in.mark(readlimit);
            }

            @Override
            public synchronized void reset() throws IOException {
                in.reset();
            }

            @Override
            public boolean markSupported() {
                return in.markSupported();
            }
        }

        OutputStream newOutputStream(String path, OpenOptions options) throws IOException {
            assert options.write;

            int mode = options.append ? ChannelSftp.APPEND : ChannelSftp.OVERWRITE;
            try {
                OutputStream out = channelSftp.put(path, mode);
                out = new SFTPOutputStream(path, out, options.deleteOnClose);
                addReference(out);
                return out;
            } catch (SftpException e) {
                throw exceptionFactory.createNewOutputStreamException(path, e, options.options);
            }
        }

        private final class SFTPOutputStream extends OutputStream {

            private final String path;
            private final OutputStream out;
            private final boolean deleteOnClose;

            private boolean open = true;

            private SFTPOutputStream(String path, OutputStream out, boolean deleteOnClose) {
                this.path = path;
                this.out = out;
                this.deleteOnClose = deleteOnClose;
                logEvent(() -> SFTPMessages.log.createdOutputStream(path));
            }

            @Override
            public void write(int b) throws IOException {
                out.write(b);
            }

            @Override
            public void write(byte[] b) throws IOException {
                out.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }

            @Override
            public void flush() throws IOException {
                out.flush();
            }

            @Override
            public void close() throws IOException {
                if (open) {
                    try {
                        out.close();
                    } finally {
                        // always finalize the stream, to prevent pool starvation
                        // set open to false as well, to prevent finalizing the stream twice
                        open = false;
                        removeReference(this);
                    }
                    if (deleteOnClose) {
                        delete(path, false);
                    }
                    logEvent(() -> SFTPMessages.log.closedOutputStream(path));
                }
            }
        }

        void storeFile(String path, InputStream local, Collection<? extends OpenOption> openOptions) throws IOException {
            try {
                channelSftp.put(local, path);
            } catch (SftpException e) {
                throw exceptionFactory.createNewOutputStreamException(path, e, openOptions);
            }
        }

        SftpATTRS readAttributes(String path, boolean followLinks) throws IOException {
            try {
                return followLinks ? channelSftp.stat(path) : channelSftp.lstat(path);
            } catch (SftpException e) {
                throw exceptionFactory.createGetFileException(path, e);
            }
        }

        String readSymbolicLink(String path) throws IOException {
            try {
                return channelSftp.readlink(path);
            } catch (SftpException e) {
                throw exceptionFactory.createReadLinkException(path, e);
            }
        }

        List<LsEntry> listFiles(String path) throws IOException {
            final List<LsEntry> entries = new ArrayList<>();
            LsEntrySelector selector = entry -> {
                entries.add(entry);
                return LsEntrySelector.CONTINUE;
            };
            try {
                channelSftp.ls(path, selector);
            } catch (SftpException e) {
                throw exceptionFactory.createListFilesException(path, e);
            }
            return entries;
        }

        void mkdir(String path) throws IOException {
            try {
                channelSftp.mkdir(path);
            } catch (SftpException e) {
                if (fileExists(path)) {
                    throw new FileAlreadyExistsException(path);
                }
                throw exceptionFactory.createCreateDirectoryException(path, e);
            }
        }

        private boolean fileExists(String path) {
            try {
                channelSftp.stat(path);
                return true;
            } catch (@SuppressWarnings("unused") SftpException e) {
                // the file actually may exist, but throw the original exception instead
                return false;
            }
        }

        void delete(String path, boolean isDirectory) throws IOException {
            try {
                if (isDirectory) {
                    channelSftp.rmdir(path);
                } else {
                    channelSftp.rm(path);
                }
            } catch (SftpException e) {
                throw exceptionFactory.createDeleteException(path, e, isDirectory);
            }
        }

        void rename(String source, String target) throws IOException {
            try {
                channelSftp.rename(source, target);
            } catch (SftpException e) {
                throw exceptionFactory.createMoveException(source, target, e);
            }
        }

        void chown(String path, int uid) throws IOException {
            try {
                channelSftp.chown(uid, path);
            } catch (SftpException e) {
                throw exceptionFactory.createSetOwnerException(path, e);
            }
        }

        void chgrp(String path, int gid) throws IOException {
            try {
                channelSftp.chgrp(gid, path);
            } catch (SftpException e) {
                throw exceptionFactory.createSetGroupException(path, e);
            }
        }

        void chmod(String path, int permissions) throws IOException {
            try {
                channelSftp.chmod(permissions, path);
            } catch (SftpException e) {
                throw exceptionFactory.createSetPermissionsException(path, e);
            }
        }

        void setMtime(String path, long mtime) throws IOException {
            try {
                channelSftp.setMtime(path, (int) mtime);
            } catch (SftpException e) {
                throw exceptionFactory.createSetModificationTimeException(path, e);
            }
        }

        SftpStatVFS statVFS(String path) throws IOException {
            try {
                return channelSftp.statVFS(path);
            } catch (SftpException e) {
                if (e.id == ChannelSftp.SSH_FX_OP_UNSUPPORTED) {
                    throw new UnsupportedOperationException(e);
                }
                // reuse the exception handling for get file
                throw exceptionFactory.createGetFileException(path, e);
            }
        }
    }

    IOException asIOException(Exception e) throws IOException {
        if (e instanceof IOException) {
            throw (IOException) e;
        }
        FileSystemException exception = new FileSystemException(null, null, e.getMessage());
        exception.initCause(e);
        throw exception;
    }
}
