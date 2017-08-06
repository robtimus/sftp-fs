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
import java.nio.file.FileSystemException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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

    private final JSch jsch = new JSch();

    private final String hostname;
    private final int port;

    private final SFTPEnvironment env;
    private final FileSystemExceptionFactory exceptionFactory;

    private final BlockingQueue<Channel> pool;

    SSHChannelPool(String hostname, int port, SFTPEnvironment env) throws IOException {
        this.hostname = hostname;
        this.port = port;
        this.env = env.clone();
        this.exceptionFactory = env.getExceptionFactory();
        final int poolSize = env.getClientConnectionCount();
        this.pool = new ArrayBlockingQueue<>(poolSize);

        try {
            for (int i = 0; i < poolSize; i++) {
                pool.add(new Channel(true));
            }
        } catch (IOException e) {
            // creating the pool failed, disconnect all channels
            for (Channel channel : pool) {
                try {
                    channel.disconnect();
                } catch (IOException e2) {
                    e.addSuppressed(e2);
                }
            }
            throw e;
        }
    }

    Channel get() throws IOException {
        try {
            Channel channel = pool.take();
            try {
                if (!channel.isConnected()) {
                    channel = new Channel(true);
                }
            } catch (final Exception e) {
                // could not create a new channel; re-add the broken channel to the pool to prevent pool starvation
                pool.add(channel);
                throw e;
            }
            channel.increaseRefCount();
            return channel;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            InterruptedIOException iioe = new InterruptedIOException(e.getMessage());
            iioe.initCause(e);
            throw iioe;
        }
    }

    Channel getOrCreate() throws IOException {
        Channel channel = pool.poll();
        if (channel == null) {
            // nothing was taken from the pool, so no risk of pool starvation if creating the channel fails
            return new Channel(false);
        }
        try {
            if (!channel.isConnected()) {
                channel = new Channel(true);
            }
        } catch (final Exception e) {
            // could not create a new channel; re-add the broken channel to the pool to prevent pool starvation
            pool.add(channel);
            throw e;
        }
        channel.increaseRefCount();
        return channel;
    }

    void keepAlive() throws IOException {
        List<Channel> channels = new ArrayList<>();
        pool.drainTo(channels);

        IOException exception = null;
        for (Channel channel : channels) {
            try {
                channel.keepAlive();
            } catch (IOException e) {
                exception = add(exception, e);
            } finally {
                returnToPool(channel);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    void close() throws IOException {
        List<Channel> channels = new ArrayList<>();
        pool.drainTo(channels);

        IOException exception = null;
        for (Channel channel : channels) {
            try {
                channel.disconnect();
            } catch (IOException e) {
                exception = add(exception, e);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    private IOException add(IOException existing, IOException e) {
        if (existing == null) {
            return e;
        }
        existing.addSuppressed(e);
        return existing;
    }

    private void returnToPool(Channel channel) {
        assert channel.refCount == 0;

        pool.add(channel);
    }

    final class Channel implements Closeable {

        private final ChannelSftp channel;
        private final boolean pooled;

        private int refCount = 0;

        private Channel(boolean pooled) throws IOException {
            this.channel = env.openChannel(jsch, hostname, port);
            this.pooled = pooled;
        }

        private void increaseRefCount() {
            refCount++;
        }

        private int decreaseRefCount() {
            if (refCount > 0) {
                refCount--;
            }
            return refCount;
        }

        private void keepAlive() throws IOException {
            try {
                channel.getSession().sendKeepAliveMsg();
            } catch (Exception e) {
                // can't be less generic as sendKeepAliveMsg declares to throw Exception
                throw asIOException(e);
            }
        }

        private boolean isConnected() {
            if (channel.isConnected()) {
                try {
                    channel.getSession().sendKeepAliveMsg();
                    return true;
                } catch (@SuppressWarnings("unused") Exception e) {
                    // the keep alive failed - treat as not connected, and actually disconnect quietly
                    disconnectQuietly();
                }
            }
            return false;
        }

        private void disconnect() throws IOException {
            channel.disconnect();
            try {
                channel.getSession().disconnect();
            } catch (JSchException e) {
                throw asIOException(e);
            }
        }

        private void disconnectQuietly() {
            channel.disconnect();
            try {
                channel.getSession().disconnect();
            } catch (@SuppressWarnings("unused") JSchException e) {
                // ignore
            }
        }

        @Override
        public void close() throws IOException {
            if (decreaseRefCount() == 0) {
                if (pooled) {
                    returnToPool(this);
                } else {
                    disconnect();
                }
            }
        }

        String pwd() throws IOException {
            try {
                return channel.pwd();
            } catch (SftpException e) {
                throw asIOException(e);
            }
        }

        @SuppressWarnings("resource")
        InputStream newInputStream(String path, OpenOptions options) throws IOException {
            assert options.read;

            try {
                InputStream in = channel.get(path);
                refCount++;
                return new SFTPInputStream(path, in, options.deleteOnClose);
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
                    in.close();
                    open = false;
                    finalizeStream();
                    if (deleteOnClose) {
                        delete(path, false);
                    }
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

        @SuppressWarnings("resource")
        OutputStream newOutputStream(String path, OpenOptions options) throws IOException {
            assert options.write;

            int mode = options.append ? ChannelSftp.APPEND : ChannelSftp.OVERWRITE;
            try {
                OutputStream out = channel.put(path, mode);
                refCount++;
                return new SFTPOutputStream(path, out, options.deleteOnClose);
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
                    out.close();
                    open = false;
                    finalizeStream();
                    if (deleteOnClose) {
                        delete(path, false);
                    }
                }
            }
        }

        private void finalizeStream() throws IOException {
            assert refCount > 0;

            if (decreaseRefCount() == 0) {
                if (pooled) {
                    returnToPool(Channel.this);
                } else {
                    disconnect();
                }
            }
        }

        void storeFile(String path, InputStream local, Collection<? extends OpenOption> openOptions) throws IOException {
            try {
                channel.put(local, path);
            } catch (SftpException e) {
                throw exceptionFactory.createNewOutputStreamException(path, e, openOptions);
            }
        }

        SftpATTRS readAttributes(String path, boolean followLinks) throws IOException {
            try {
                return followLinks ? channel.stat(path) : channel.lstat(path);
            } catch (SftpException e) {
                throw exceptionFactory.createGetFileException(path, e);
            }
        }

        String readSymbolicLink(String path) throws IOException {
            try {
                return channel.readlink(path);
            } catch (SftpException e) {
                throw exceptionFactory.createReadLinkException(path, e);
            }
        }

        List<LsEntry> listFiles(String path) throws IOException {
            final List<LsEntry> entries = new ArrayList<>();
            LsEntrySelector selector = new LsEntrySelector() {
                @Override
                public int select(LsEntry entry) {
                    entries.add(entry);
                    return CONTINUE;
                }
            };
            try {
                channel.ls(path, selector);
            } catch (SftpException e) {
                throw exceptionFactory.createListFilesException(path, e);
            }
            return entries;
        }

        void mkdir(String path) throws IOException {
            try {
                channel.mkdir(path);
            } catch (SftpException e) {
                throw exceptionFactory.createCreateDirectoryException(path, e);
            }
        }

        void delete(String path, boolean isDirectory) throws IOException {
            try {
                if (isDirectory) {
                    channel.rmdir(path);
                } else {
                    channel.rm(path);
                }
            } catch (SftpException e) {
                throw exceptionFactory.createDeleteException(path, e, isDirectory);
            }
        }

        void rename(String source, String target) throws IOException {
            try {
                channel.rename(source, target);
            } catch (SftpException e) {
                throw exceptionFactory.createMoveException(source, target, e);
            }
        }

        void chown(String path, int uid) throws IOException {
            try {
                channel.chown(uid, path);
            } catch (SftpException e) {
                throw exceptionFactory.createSetOwnerException(path, e);
            }
        }

        void chgrp(String path, int gid) throws IOException {
            try {
                channel.chgrp(gid, path);
            } catch (SftpException e) {
                throw exceptionFactory.createSetGroupException(path, e);
            }
        }

        void chmod(String path, int permissions) throws IOException {
            try {
                channel.chmod(permissions, path);
            } catch (SftpException e) {
                throw exceptionFactory.createSetPermissionsException(path, e);
            }
        }

        void setMtime(String path, long mtime) throws IOException {
            try {
                channel.setMtime(path, (int) mtime);
            } catch (SftpException e) {
                throw exceptionFactory.createSetModificationTimeException(path, e);
            }
        }

        SftpStatVFS statVFS(String path) throws IOException {
            try {
                return channel.statVFS(path);
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
