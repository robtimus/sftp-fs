/*
 * SFTPFileSystemProviderTest.java
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

import static com.github.robtimus.filesystems.sftp.SFTPFileSystemProvider.normalizeWithUsername;
import static com.github.robtimus.filesystems.sftp.SFTPFileSystemProvider.normalizeWithoutPassword;
import static com.github.robtimus.junit.support.ThrowableAssertions.assertChainEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.ProviderMismatchException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import com.github.robtimus.filesystems.Messages;
import com.github.robtimus.filesystems.URISupport;

@SuppressWarnings("nls")
class SFTPFileSystemProviderTest extends AbstractSFTPFileSystemTest {

    @Nested
    class PathsAndFiles {

        @Test
        void testSuccess() throws IOException {
            try (SFTPFileSystem fs = newFileSystem(createEnv())) {
                Path path = Paths.get(URI.create(getBaseUrl() + "/foo"));
                assertThat(path, instanceOf(SFTPPath.class));
                // as required by Paths.get
                assertEquals(path, path.toAbsolutePath());

                // the file does not exist yet
                assertFalse(Files.exists(path));

                Files.createFile(path);
                try {
                    // the file now exists
                    assertTrue(Files.exists(path));

                    byte[] content = new byte[1024];
                    new Random().nextBytes(content);
                    try (OutputStream output = Files.newOutputStream(path)) {
                        output.write(content);
                    }

                    // check the file directly
                    Path file = getFile("/foo");
                    assertArrayEquals(content, getContents(file));

                } finally {

                    Files.delete(path);
                    assertFalse(Files.exists(path));

                    assertFalse(Files.exists(getPath("/foo")));
                }
            }
        }

        @Test
        void testFileSystemNotFound() {
            URI uri = getURI();
            FileSystemNotFoundException exception = assertThrows(FileSystemNotFoundException.class, () -> Paths.get(uri));
            assertEquals(normalizeWithUsername(uri, null).toString(), exception.getMessage());
            assertEquals(normalizeWithoutPassword(uri).toString(), exception.getMessage());
        }
    }

    @Nested
    class NewFileSystem {

        @Test
        void testWithMinimalEnv() throws IOException {
            URI uri = URI.create(getBaseUrlWithCredentials() + getDefaultDir());
            try (FileSystem fs = FileSystems.newFileSystem(uri, createMinimalEnv())) {
                Path path = fs.getPath("");
                assertEquals(getDefaultDir(), path.toAbsolutePath().toString());
            }
        }

        @Test
        void testWithMinimalIdentityEnv() throws IOException {
            URI uri = URI.create(getBaseUrl() + getDefaultDir());
            try (FileSystem fs = FileSystems.newFileSystem(uri, createMinimalIdentityEnv())) {
                Path path = fs.getPath("");
                assertEquals(getDefaultDir(), path.toAbsolutePath().toString());
            }
        }

        @Test
        void testWithUserInfoAndCredentials() {
            URI uri = URI.create(getBaseUrl());
            SFTPEnvironment env = createEnv();
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> FileSystems.newFileSystem(uri, env));
            assertChainEquals(Messages.uri().hasUserInfo(uri), exception);
        }

        @Test
        void testWithPathAndDefaultDir() {
            URI uri = getURI().resolve("/path");
            SFTPEnvironment env = createEnv();
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> FileSystems.newFileSystem(uri, env));
            assertChainEquals(Messages.uri().hasPath(uri), exception);
        }

        @Test
        void testWithQuery() {
            URI uri = getURI().resolve("?q=v");
            SFTPEnvironment env = createEnv();
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> FileSystems.newFileSystem(uri, env));
            assertChainEquals(Messages.uri().hasQuery(uri), exception);
        }

        @Test
        void testWithFragment() {
            URI uri = getURI().resolve("#id");
            SFTPEnvironment env = createEnv();
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> FileSystems.newFileSystem(uri, env));
            assertChainEquals(Messages.uri().hasFragment(uri), exception);
        }
    }

    @Nested
    class GetFileSystem {

        @Test
        @SuppressWarnings("resource")
        void testExisting() throws IOException {
            try (FileSystem fs = newFileSystem(createEnv())) {
                FileSystem existingFileSystem = FileSystems.getFileSystem(URI.create(getBaseUrl()));
                assertSame(fs, existingFileSystem);

                existingFileSystem = FileSystems.getFileSystem(URI.create(getBaseUrl() + "/"));
                assertSame(fs, existingFileSystem);
            }
        }

        @Test
        @SuppressWarnings("resource")
        void testExistingWithTrailingSlash() throws IOException {
            try (FileSystem fs = newFileSystem(createEnv())) {
                FileSystem existingFileSystem = FileSystems.getFileSystem(URI.create(getBaseUrl() + "/"));
                assertSame(fs, existingFileSystem);

                existingFileSystem = FileSystems.getFileSystem(URI.create(getBaseUrl() + "/"));
                assertSame(fs, existingFileSystem);
            }
        }

        @Test
        void testWithNonEmptyPath() {
            URI uri = URI.create(getBaseUrl() + "/foo");
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> FileSystems.getFileSystem(uri));
            assertChainEquals(Messages.uri().hasPath(uri), exception);
        }

        @Test
        void testNotExisting() {
            URI uri = URI.create(getBaseUrl());
            assertThrows(FileSystemNotFoundException.class, () -> FileSystems.getFileSystem(uri));
        }

        @Test
        @SuppressWarnings("resource")
        void testClosed() throws IOException {
            try (FileSystem fs = newFileSystem(createEnv())) {
                URI uri = URI.create(getBaseUrl());
                FileSystem existingFileSystem = FileSystems.getFileSystem(uri);
                assertSame(fs, existingFileSystem);

                fs.close();
                assertThrows(FileSystemNotFoundException.class, () -> FileSystems.getFileSystem(uri));
            }
        }
    }

    @Test
    void testRemoveFileSystem() throws IOException {
        addDirectory("/foo/bar");

        SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
        SFTPEnvironment environment = createEnv();
        URI uri;
        try (SFTPFileSystem fs = newFileSystem(provider, environment)) {
            SFTPPath path = new SFTPPath(fs, "/foo/bar");

            uri = path.toUri();

            assertFalse(provider.isHidden(path));
        }
        FileSystemNotFoundException exception = assertThrows(FileSystemNotFoundException.class, () -> provider.getPath(uri));
        assertEquals(normalizeWithUsername(uri, environment.getUsername()).toString(), exception.getMessage());
        assertEquals(normalizeWithoutPassword(uri).toString(), exception.getMessage());
    }

    @Nested
    class NormalizeWithoutPassword {

        @Test
        void testMinimalURI() {
            URI uri = getURI();
            assertSame(uri, SFTPFileSystemProvider.normalizeWithoutPassword(uri));
        }

        @Test
        void testWithOnlyUserName() {
            URI uri = URI.create(getBaseUrl());
            assertEquals(uri, SFTPFileSystemProvider.normalizeWithoutPassword(uri));
        }

        @Test
        void testWithUserNameAndPassword() {
            URI uri = URI.create(getBaseUrlWithCredentials());
            URI expected = URI.create(getBaseUrl());
            assertEquals(expected, SFTPFileSystemProvider.normalizeWithoutPassword(uri));
        }

        @Test
        void testWithPath() {
            testNormalizeWithoutPassword("/");
        }

        @Test
        void testWithQuery() {
            testNormalizeWithoutPassword("?q=v");
        }

        @Test
        void testWithFragment() {
            testNormalizeWithoutPassword("#id");
        }

        private void testNormalizeWithoutPassword(String uriAddition) {
            URI uri = getURI().resolve(uriAddition);
            assertEquals(getURI(), SFTPFileSystemProvider.normalizeWithoutPassword(uri));
        }
    }

    @Nested
    class NormalizeWithUsername {

        @Test
        void testMinimalURIWithoutUserInfo() {
            URI uri = getURI();
            assertSame(uri, SFTPFileSystemProvider.normalizeWithUsername(uri, null));
        }

        @Test
        void testMinimalURIWithUsername() {
            URI uri = getURI();
            assertEquals(URI.create(getBaseUrl()), SFTPFileSystemProvider.normalizeWithUsername(uri, getUsername()));
        }

        @Test
        void testWithPath() {
            testNormalizeWithoutPassword("/");
        }

        @Test
        void testWithQuery() {
            testNormalizeWithoutPassword("?q=v");
        }

        @Test
        void testWithFragment() {
            testNormalizeWithoutPassword("#id");
        }

        private void testNormalizeWithoutPassword(String uriAddition) {
            URI uri = getURI().resolve(uriAddition);
            assertEquals(URI.create(getBaseUrl()), SFTPFileSystemProvider.normalizeWithUsername(uri, getUsername()));
        }
    }

    @Nested
    class GetPath {

        @Test
        void testSuccess() throws IOException {
            Map<String, String> inputs = new HashMap<>();
            inputs.put("/", "/");
            inputs.put("foo", "/home/foo");
            inputs.put("/foo", "/foo");
            inputs.put("foo/bar", "/home/foo/bar");
            inputs.put("/foo/bar", "/foo/bar");

            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            try (SFTPFileSystem fs = newFileSystem(provider, createEnv())) {
                for (Map.Entry<String, String> entry : inputs.entrySet()) {
                    URI uri = fs.getPath(entry.getKey()).toUri();
                    Path path = provider.getPath(uri);
                    assertThat(path, instanceOf(SFTPPath.class));
                    assertEquals(entry.getValue(), ((SFTPPath) path).path());
                }
                for (Map.Entry<String, String> entry : inputs.entrySet()) {
                    URI uri = fs.getPath(entry.getKey()).toUri();
                    uri = URISupport.create(uri.getScheme().toUpperCase(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(),
                            null, null);
                    Path path = provider.getPath(uri);
                    assertThat(path, instanceOf(SFTPPath.class));
                    assertEquals(entry.getValue(), ((SFTPPath) path).path());
                }
            }
        }

        @Test
        void testNoScheme() {
            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            URI uri = URI.create("/foo/bar");
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> provider.getPath(uri));
            assertChainEquals(Messages.uri().notAbsolute(uri), exception);
        }

        @Test
        void testInvalidScheme() {
            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            URI uri = URI.create("https://www.github.com/");
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> provider.getPath(uri));
            assertChainEquals(Messages.uri().invalidScheme(uri, "sftp"), exception);
        }

        @Test
        void testFileSystemCreatedWithPath() {
            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            URI uri = URI.create(getBaseUrlWithCredentials() + "/foo");
            SFTPEnvironment.setDefault(createMinimalEnv());
            try {
                Path path = assertDoesNotThrow(() -> provider.getPath(uri));
                assertNotEquals(uri, path.toUri());
                assertEquals("/foo", path.toAbsolutePath().toString());
                assertFalse(Files.exists(path));
                assertDoesNotThrow(() -> path.getFileSystem().close());
            } finally {
                SFTPEnvironment.setDefault(null);
            }
        }

        @Test
        void testFileSystemCreatedWithoutPath() {
            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            URI uri = URI.create(getBaseUrlWithCredentials());
            SFTPEnvironment.setDefault(createMinimalEnv()
                    .withDefaultDirectory(getDefaultDir()));
            try {
                Path path = assertDoesNotThrow(() -> provider.getPath(uri));
                assertNotEquals(uri, path.toUri());
                assertEquals(getDefaultDir(), path.toAbsolutePath().toString());
                assertTrue(Files.exists(path));
                assertDoesNotThrow(() -> path.getFileSystem().close());
            } finally {
                SFTPEnvironment.setDefault(null);
            }
        }

        @Test
        void testFileSystemCreatedWithIdentity() {
            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            URI uri = URI.create(getBaseUrl() + "/foo");
            SFTPEnvironment.setDefault(createMinimalIdentityEnv());
            try {
                Path path = assertDoesNotThrow(() -> provider.getPath(uri));
                assertEquals(uri, path.toUri());
                assertEquals("/foo", path.toAbsolutePath().toString());
                assertFalse(Files.exists(path));
                assertDoesNotThrow(() -> path.getFileSystem().close());
            } finally {
                SFTPEnvironment.setDefault(null);
            }
        }

        @Test
        void testFileSystemCreationFailure() {
            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            URI uri = URI.create(getBaseUrlWithCredentials());
            // Cause: unknown host key
            FileSystemNotFoundException exception = assertThrows(FileSystemNotFoundException.class, () -> provider.getPath(uri));
            assertEquals(normalizeWithoutPassword(uri).toString(), exception.getMessage());
        }
    }

    @Nested
    class IsSameFile {

        @Test
        void testWithDifferentTypes() throws IOException {

            SFTPFileSystemProvider sftpProvider = new SFTPFileSystemProvider();

            @SuppressWarnings("resource")
            FileSystem defaultFileSystem = FileSystems.getDefault();
            FileSystemProvider defaultProvider = defaultFileSystem.provider();

            try (SFTPFileSystem fs1 = newFileSystem(sftpProvider, createEnv())) {
                SFTPPath path1 = new SFTPPath(fs1, "pom.xml");
                Path path2 = Paths.get("pom.xml");

                assertFalse(sftpProvider.isSameFile(path1, path2));
                assertFalse(defaultProvider.isSameFile(path2, path1));
            }
        }
    }

    @Nested
    class GetFileAttributeView {

        @Test
        void testBasic() throws IOException {
            Path foo = addDirectory("/foo/bar");

            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            try (SFTPFileSystem fs = newFileSystem(provider, createEnv())) {
                SFTPPath path = new SFTPPath(fs, "/foo/bar");

                BasicFileAttributeView view = fs.provider().getFileAttributeView(path, BasicFileAttributeView.class);
                assertNotNull(view);
                assertEquals("basic", view.name());

                assertDoesNotThrow(() -> view.setTimes(null, null, null));

                FileTime fileTime = FileTime.fromMillis(0);

                view.setTimes(fileTime, null, null);
                assertEquals(fileTime, Files.readAttributes(foo, BasicFileAttributes.class).lastModifiedTime());

                IOException exception = assertThrows(IOException.class, () -> view.setTimes(null, fileTime, null));
                IllegalArgumentException cause = assertInstanceOf(IllegalArgumentException.class, exception.getCause());
                assertChainEquals(Messages.fileSystemProvider().unsupportedFileAttribute("lastAccessTime"), cause);

                exception = assertThrows(IOException.class, () -> view.setTimes(null, null, fileTime));
                cause = assertInstanceOf(IllegalArgumentException.class, exception.getCause());
                assertChainEquals(Messages.fileSystemProvider().unsupportedFileAttribute("creationTime"), cause);
            }
        }

        @Test
        void testFileOwner() throws IOException {
            addDirectory("/foo/bar");

            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            try (SFTPFileSystem fs = newFileSystem(provider, createEnv())) {
                SFTPPath path = new SFTPPath(fs, "/foo/bar");

                FileOwnerAttributeView view = fs.provider().getFileAttributeView(path, FileOwnerAttributeView.class);
                assertNotNull(view);

                assertNotNull(view.getOwner());
                assertNotNull(view.getOwner().getName());
            }
        }

        @Test
        void testPosix() throws IOException {
            Path foo = addDirectory("/foo/bar");

            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            try (SFTPFileSystem fs = newFileSystem(provider, createEnv())) {
                SFTPPath path = new SFTPPath(fs, "/foo/bar");

                PosixFileAttributeView view = fs.provider().getFileAttributeView(path, PosixFileAttributeView.class);
                assertNotNull(view);
                assertEquals("posix", view.name());

                assertNotNull(view.getOwner());
                assertNotNull(view.getOwner().getName());

                FileTime fileTime = FileTime.fromMillis(0);

                view.setTimes(fileTime, null, null);
                assertEquals(fileTime, Files.readAttributes(foo, BasicFileAttributes.class).lastModifiedTime());

                IOException exception = assertThrows(IOException.class, () -> view.setTimes(null, fileTime, null));
                IllegalArgumentException cause = assertInstanceOf(IllegalArgumentException.class, exception.getCause());
                assertChainEquals(Messages.fileSystemProvider().unsupportedFileAttribute("lastAccessTime"), cause);

                exception = assertThrows(IOException.class, () -> view.setTimes(null, null, fileTime));
                cause = assertInstanceOf(IllegalArgumentException.class, exception.getCause());
                assertChainEquals(Messages.fileSystemProvider().unsupportedFileAttribute("creationTime"), cause);
            }
        }

        @Test
        void testOther() throws IOException {
            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            try (SFTPFileSystem fs = newFileSystem(provider, createEnv())) {
                SFTPPath path = new SFTPPath(fs, "/foo/bar");

                DosFileAttributeView view = fs.provider().getFileAttributeView(path, DosFileAttributeView.class);
                assertNull(view);
            }
        }

        @Test
        void testReadAttributes() throws IOException {
            addDirectory("/foo/bar");

            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            try (SFTPFileSystem fs = newFileSystem(provider, createEnv())) {
                SFTPPath path = new SFTPPath(fs, "/foo/bar");

                BasicFileAttributeView view = fs.provider().getFileAttributeView(path, BasicFileAttributeView.class);
                assertNotNull(view);

                BasicFileAttributes attributes = view.readAttributes();
                assertTrue(attributes.isDirectory());
            }
        }
    }

    @Nested
    class KeepAlive {

        @Test
        void testWithFTPFileSystem() throws IOException {
            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            try (SFTPFileSystem fs = newFileSystem(provider, createEnv())) {
                assertDoesNotThrow(() -> SFTPFileSystemProvider.keepAlive(fs));
            }
        }

        @Test
        void testWithNonFTPFileSystem() {
            @SuppressWarnings("resource")
            FileSystem defaultFileSystem = FileSystems.getDefault();
            assertThrows(ProviderMismatchException.class, () -> SFTPFileSystemProvider.keepAlive(defaultFileSystem));
        }

        @Test
        void testWithNullFTPFileSystem() {
            assertThrows(ProviderMismatchException.class, () -> SFTPFileSystemProvider.keepAlive(null));
        }
    }

    @Nested
    class CreateDirectory {

        @Test
        void testThroughCreateDirectories() throws IOException {
            addDirectory("/foo/bar");

            SFTPFileSystemProvider provider = new SFTPFileSystemProvider();
            try (SFTPFileSystem fs = newFileSystem(provider, createEnv())) {
                SFTPPath path = new SFTPPath(fs, "/foo/bar");
                Files.createDirectories(path);
            }

            assertTrue(Files.exists(getPath("/foo/bar")));
        }
    }

    private SFTPFileSystem newFileSystem(Map<String, ?> env) throws IOException {
        return (SFTPFileSystem) FileSystems.newFileSystem(getURI(), env);
    }

    private SFTPFileSystem newFileSystem(SFTPFileSystemProvider provider, Map<String, ?> env) throws IOException {
        return (SFTPFileSystem) provider.newFileSystem(getURI(), env);
    }
}
