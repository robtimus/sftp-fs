/*
 * AbstractSFTPFileSystemTest.java
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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystemException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.common.keyprovider.MappedKeyPairProvider;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import com.github.robtimus.filesystems.sftp.server.FixedSftpSubsystem;
import com.jcraft.jsch.SftpException;

@SuppressWarnings("nls")
abstract class AbstractSFTPFileSystemTest {

    private static final String USERNAME = "TEST_USER";
    private static final String PASSWORD = "TEST_PASSWORD";
    private static final PublicKey PUBLIC_KEY = readPublicKey("id_rsa.pkcs8");
    private static final PublicKey PUBLIC_KEY_NOPASS = readPublicKey("id_rsa_nopass.pkcs8");

    private static int port;
    private static SshServer sshServer;
    private static Path rootPath;
    private static Path defaultDir;
    private static ExceptionFactoryWrapper exceptionFactory;
    private static SFTPFileSystem sftpFileSystem;
    private static SFTPFileSystem sftpFileSystem2;

    protected final SFTPFileSystem fileSystem = sftpFileSystem;
    protected final SFTPFileSystem fileSystem2 = sftpFileSystem2;

    private static PublicKey readPublicKey(String resource) {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (InputStream input = AbstractSFTPFileSystemTest.class.getResourceAsStream(resource)) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = input.read(buffer)) != -1) {
                    output.write(buffer, 0, len);
                }
            }
            // public key parsing based on https://gist.github.com/destan/b708d11bd4f403506d6d5bb5fe6a82c5
            String publicKeyContent = new String(output.toString("UTF-8"))
                    .replace("\\n", "")
                    .replace("-----BEGIN PUBLIC KEY-----", "")
                    .replace("-----END PUBLIC KEY-----", "");
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            X509EncodedKeySpec keySpec = new X509EncodedKeySpec(Base64.getMimeDecoder().decode(publicKeyContent));
            return keyFactory.generatePublic(keySpec);

        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    @BeforeAll
    static void setupClass() throws NoSuchAlgorithmException, IOException {
        setupClass(new FixedSftpSubsystem.Factory());
    }

    protected static void setupClass(SftpSubsystemFactory subSystemFactory) throws NoSuchAlgorithmException, IOException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        KeyPair keyPair = generator.generateKeyPair();

        port = findFreePort();

        sshServer = SshServer.setUpDefaultServer();
        sshServer.setPort(port);
        sshServer.setKeyPairProvider(new MappedKeyPairProvider(keyPair));
        sshServer.setPasswordAuthenticator((String username, String password, ServerSession session) ->
                USERNAME.equals(username) && PASSWORD.equals(password));
        sshServer.setPublickeyAuthenticator((String username, PublicKey key, ServerSession session) ->
                USERNAME.equals(username) && (PUBLIC_KEY.equals(key) || PUBLIC_KEY_NOPASS.equals(key)));
        sshServer.setSubsystemFactories(Arrays.asList(subSystemFactory));

        rootPath = Files.createTempDirectory("sftp-fs");
        defaultDir = rootPath.resolve("home");
        Files.createDirectory(defaultDir);
        VirtualFileSystemFactory fsFactory = new VirtualFileSystemFactory(rootPath);

        sshServer.setFileSystemFactory(fsFactory);

        sshServer.start();

        exceptionFactory = new ExceptionFactoryWrapper();
        exceptionFactory.delegate = DefaultFileSystemExceptionFactory.INSTANCE;
        sftpFileSystem = createFileSystem();
        sftpFileSystem2 = createFileSystem(3);
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @AfterAll
    static void cleanupClass() throws IOException {
        Files.deleteIfExists(defaultDir);
        Files.deleteIfExists(rootPath);

        sftpFileSystem.close();

        sshServer.stop();
        sshServer = null;
    }

    private static SFTPFileSystem createFileSystem() throws IOException {
        Map<String, ?> env = createEnv();
        return (SFTPFileSystem) new SFTPFileSystemProvider().newFileSystem(URI.create("sftp://localhost:" + port), env);
    }

    private static SFTPFileSystem createFileSystem(int maxSize) throws IOException {
        Map<String, ?> env = createEnv().withPoolConfig(SFTPPoolConfig.custom().withMaxSize(maxSize).build());
        return (SFTPFileSystem) new SFTPFileSystemProvider().newFileSystem(URI.create("sftp://localhost:" + port), env);
    }

    protected static SFTPEnvironment createEnv() {
        return new SFTPEnvironment()
                .withUsername(USERNAME)
                .withUserInfo(new SimpleUserInfo(PASSWORD.toCharArray()))
                .withHostKeyRepository(TrustAllHostKeyRepository.INSTANCE)
                .withDefaultDirectory(defaultDir.getFileName().toString())
                .withPoolConfig(SFTPPoolConfig.custom().withMaxSize(1).build())
                .withFileSystemExceptionFactory(exceptionFactory);
    }

    @BeforeEach
    void setup() throws IOException {
        Files.createDirectories(defaultDir);

        exceptionFactory.delegate = spy(DefaultFileSystemExceptionFactory.INSTANCE);
    }

    @AfterEach
    void cleanup() throws IOException {
        exceptionFactory.delegate = null;

        purgePath(rootPath);
    }

    private void purgePath(final Path path) throws IOException {

        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                dir.toFile().setWritable(true, false);
                return super.preVisitDirectory(dir, attrs);
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                file.toFile().setWritable(true, false);
                Files.delete(file);
                return super.visitFile(file, attrs);
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (!rootPath.equals(dir) && !defaultDir.equals(dir)) {
                    Files.delete(dir);
                }
                return super.postVisitDirectory(dir, exc);
            }
        });
    }

    protected final String getBaseUrl() {
        return "sftp://" + USERNAME + "@localhost:" + port;
    }

    protected final URI getURI() {
        return URI.create("sftp://localhost:" + port);
    }

    protected final SFTPPath createPath(String path) {
        return new SFTPPath(sftpFileSystem, path);
    }

    protected final SFTPPath createPath(SFTPFileSystem fs, String path) {
        return new SFTPPath(fs, path);
    }

    protected final FileSystemExceptionFactory getExceptionFactory() {
        return exceptionFactory.delegate;
    }

    protected final Path getPath(String path) {
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        return rootPath.resolve(path);
    }

    protected final Path getFile(String path) {
        Path result = getPath(path);
        assertTrue(Files.isRegularFile(result, LinkOption.NOFOLLOW_LINKS));
        return result;
    }

    protected final Path getDirectory(String path) {
        Path result = getPath(path);
        assertTrue(Files.isDirectory(result, LinkOption.NOFOLLOW_LINKS));
        return result;
    }

    protected final Path getSymLink(String path) {
        Path result = getPath(path);
        assertTrue(Files.isSymbolicLink(result));
        return result;
    }

    protected final Path addFile(String path) throws IOException {
        Path file = getPath(path);
        Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.createFile(file);
        Files.write(file, "Hello world".getBytes());
        return file;
    }

    protected final Path addDirectory(String path) throws IOException {
        Path directory = getPath(path);
        Path parent = directory.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.createDirectory(directory);
        return directory;
    }

    protected final Path addSymLink(String path, Path target) throws IOException {
        Path symLink = getPath(path);
        Path parent = symLink.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.createSymbolicLink(symLink, target);
        return symLink;
    }

    protected final boolean delete(String path) throws IOException {
        Path dir = getPath(path);
        try {
            purgePath(dir);
            return true;
        } catch (NoSuchFileException e) {
            if (e.getFile().equals(dir.toString())) {
                return false;
            }
            throw e;
        }
    }

    protected final int getChildCount(String path) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(getPath(path))) {
            int count = 0;
            for (Iterator<?> i = stream.iterator(); i.hasNext(); ) {
                i.next();
                count++;
            }
            return count;
        }
    }

    protected final byte[] getContents(Path file) throws IOException {
        return Files.readAllBytes(file);
    }

    protected final String getStringContents(Path file) throws IOException {
        return new String(getContents(file), StandardCharsets.UTF_8);
    }

    protected final void setContents(Path file, byte[] contents) throws IOException {
        try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            out.write(contents);
        }
    }

    protected final void setContents(Path file, String contents) throws IOException {
        setContents(file, contents.getBytes(StandardCharsets.UTF_8));
    }

    protected final long getTotalSize() throws IOException {
        return getTotalSize(rootPath);
    }

    private long getTotalSize(Path path) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        long size = attributes.size();
        if (attributes.isDirectory()) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (Path p : stream) {
                    size += getTotalSize(p);
                }
            }
        }
        return size;
    }

    private static class ExceptionFactoryWrapper implements FileSystemExceptionFactory {

        private FileSystemExceptionFactory delegate;

        @Override
        public FileSystemException createGetFileException(String file, SftpException exception) {
            return delegate.createGetFileException(file, exception);
        }

        @Override
        public FileSystemException createReadLinkException(String link, SftpException exception) {
            return delegate.createReadLinkException(link, exception);
        }

        @Override
        public FileSystemException createListFilesException(String directory, SftpException exception) {
            return delegate.createListFilesException(directory, exception);
        }

        @Override
        public FileSystemException createChangeWorkingDirectoryException(String directory, SftpException exception) {
            return delegate.createChangeWorkingDirectoryException(directory, exception);
        }

        @Override
        public FileSystemException createCreateDirectoryException(String directory, SftpException exception) {
            return delegate.createCreateDirectoryException(directory, exception);
        }

        @Override
        public FileSystemException createDeleteException(String file, SftpException exception, boolean isDirectory) {
            return delegate.createDeleteException(file, exception, isDirectory);
        }

        @Override
        public FileSystemException createNewInputStreamException(String file, SftpException exception) {
            return delegate.createNewInputStreamException(file, exception);
        }

        @Override
        public FileSystemException createNewOutputStreamException(String file, SftpException exception, Collection<? extends OpenOption> options) {
            return delegate.createNewOutputStreamException(file, exception, options);
        }

        @Override
        public FileSystemException createCopyException(String file, String other, SftpException exception) {
            return delegate.createCopyException(file, other, exception);
        }

        @Override
        public FileSystemException createMoveException(String file, String other, SftpException exception) {
            return delegate.createMoveException(file, other, exception);
        }

        @Override
        public FileSystemException createSetOwnerException(String file, SftpException exception) {
            return delegate.createSetOwnerException(file, exception);
        }

        @Override
        public FileSystemException createSetGroupException(String file, SftpException exception) {
            return delegate.createSetGroupException(file, exception);
        }

        @Override
        public FileSystemException createSetPermissionsException(String file, SftpException exception) {
            return delegate.createSetPermissionsException(file, exception);
        }

        @Override
        public FileSystemException createSetModificationTimeException(String file, SftpException exception) {
            return delegate.createSetModificationTimeException(file, exception);
        }
    }
}
