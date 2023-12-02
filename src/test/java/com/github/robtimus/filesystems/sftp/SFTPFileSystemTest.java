/*
 * SFTPFileSystemTest.java
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

import static com.github.robtimus.filesystems.SimpleAbstractPath.CURRENT_DIR;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import com.github.robtimus.filesystems.Messages;
import com.github.robtimus.filesystems.attribute.FileAttributeViewMetadata;
import com.github.robtimus.filesystems.attribute.SimpleGroupPrincipal;
import com.github.robtimus.filesystems.attribute.SimpleUserPrincipal;
import com.jcraft.jsch.SftpException;

@SuppressWarnings("nls")
class SFTPFileSystemTest extends AbstractSFTPFileSystemTest {

    @Test
    void testGetPath() {
        testGetPath("/", "/");
        testGetPath("/foo/bar", "/", "/foo", "/bar");
        testGetPath("/foo/../bar", "/foo/", "../bar");
    }

    private void testGetPath(String path, String first, String... more) {
        SFTPPath expected = createPath(path);
        Path actual = fileSystem.getPath(first, more);
        assertEquals(expected, actual);
    }

    @Test
    void testKeepAlive() {
        assertDoesNotThrow(fileSystem::keepAlive);
    }

    @Test
    void testToUri() {
        final String prefix = getBaseUrl();

        testToUri("/", prefix + "/");
        testToUri("/foo/bar", prefix + "/foo/bar");
        testToUri("/foo/../bar", prefix + "/bar");

        testToUri("", prefix + "/home");
        testToUri("foo/bar", prefix + "/home/foo/bar");
        testToUri("foo/../bar", prefix + "/home/bar");
    }

    private void testToUri(String path, String expected) {
        URI expectedUri = URI.create(expected);
        URI actual = createPath(path).toUri();
        assertEquals(expectedUri, actual);
    }

    @Test
    void testToAbsolutePath() {

        testToAbsolutePath("/", "/");
        testToAbsolutePath("/foo/bar", "/foo/bar");
        testToAbsolutePath("/foo/../bar", "/foo/../bar");

        testToAbsolutePath("", "/home");
        testToAbsolutePath("foo/bar", "/home/foo/bar");
        testToAbsolutePath("foo/../bar", "/home/foo/../bar");
    }

    private void testToAbsolutePath(String path, String expected) {
        SFTPPath expectedPath = createPath(expected);
        Path actual = createPath(path).toAbsolutePath();
        assertEquals(expectedPath, actual);
    }

    @Nested
    class ToRealPath {

        @Test
        void testNoFollowLinks() throws IOException {
            Path foo = addDirectory("/foo");
            addDirectory("/foo/bar");
            addDirectory("/bar");
            addFile("/home/foo/bar");
            Path bar = addFile("/home/bar");

            // symbolic links
            Path symLink = addSymLink("/hello", foo);
            addSymLink("/world", symLink);
            symLink = addSymLink("/home/baz", bar);
            addSymLink("/baz", symLink);

            testNoFollowLinks("/", "/");
            testNoFollowLinks("/foo/bar", "/foo/bar");
            testNoFollowLinks("/foo/../bar", "/bar");

            testNoFollowLinks("", "/home");
            testNoFollowLinks("foo/bar", "/home/foo/bar");
            testNoFollowLinks("foo/../bar", "/home/bar");
            testNoFollowLinks(CURRENT_DIR, "/home");

            // symbolic links
            testNoFollowLinks("/hello", "/hello");
            testNoFollowLinks("/world", "/world");
            testNoFollowLinks("/home/baz", "/home/baz");
            testNoFollowLinks("/baz", "/baz");
        }

        private void testNoFollowLinks(String path, String expected) throws IOException {
            SFTPPath expectedPath = createPath(expected);
            Path actual = createPath(path).toRealPath(LinkOption.NOFOLLOW_LINKS);
            assertEquals(expectedPath, actual);
        }

        @Test
        void testFollowLinks() throws IOException {
            Path foo = addDirectory("/foo");
            addDirectory("/foo/bar");
            addDirectory("/bar");
            addFile("/home/foo/bar");
            Path bar = addFile("/home/bar");

            // symbolic links
            Path symLink = addSymLink("/hello", foo);
            addSymLink("/world", symLink);
            symLink = addSymLink("/home/baz", bar);
            addSymLink("/baz", symLink);

            testFollowLinks("/", "/");
            testFollowLinks("/foo/bar", "/foo/bar");
            testFollowLinks("/foo/../bar", "/bar");

            testFollowLinks("", "/home");
            testFollowLinks("foo/bar", "/home/foo/bar");
            testFollowLinks("foo/../bar", "/home/bar");

            // symbolic links
            testFollowLinks("/hello", "/foo");
            testFollowLinks("/world", "/foo");
            testFollowLinks("/home/baz", "/home/bar");
            testFollowLinks("/baz", "/home/bar");
        }

        private void testFollowLinks(String path, String expected) throws IOException {
            SFTPPath expectedPath = createPath(expected);
            Path actual = createPath(path).toRealPath();
            assertEquals(expectedPath, actual);
        }

        @Test
        void testBrokenLink() throws IOException {
            addSymLink("/foo", getPath("/bar"));

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> createPath("/foo").toRealPath());
            assertEquals("/bar", exception.getFile());
        }

        @Test
        void testNotExisting() {
            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> createPath("/foo").toRealPath());
            assertEquals("/foo", exception.getFile());
        }
    }

    @Nested
    class NewInputStream {

        @Test
        void testSuccess() throws IOException {
            addFile("/foo/bar");

            try (InputStream input = provider().newInputStream(createPath("/foo/bar"))) {
                // don't do anything with the stream, there's a separate test for that
            }
            // verify that the file system can be used after closing the stream
            provider().checkAccess(createPath("/foo/bar"));
        }

        @Test
        void testDeleteOnClose() throws IOException {
            addFile("/foo/bar");

            OpenOption[] options = { StandardOpenOption.DELETE_ON_CLOSE };
            try (InputStream input = provider().newInputStream(createPath("/foo/bar"), options)) {
                // don't do anything with the stream, there's a separate test for that
            }
            assertFalse(Files.exists(getPath("/foo/bar")));
            assertEquals(0, getChildCount("/foo"));
        }

        @Test
        void testDirectory() {
            testDirectory("/home", "/home");
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testCurrentDirectory(String dir) {
            testDirectory(dir, getDefaultDir());
        }

        private void testDirectory(String dir, String expectedFile) {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath(dir);

            // On Windows this will throw an AccessDeniedException when calling provider.newByteChannel
            // On Linux however that call succeeds, but the backing InputStream is broken
            IOException exception = assertThrows(IOException.class, () -> newInputStream(provider, path));
            if (OS.current() == OS.WINDOWS) {
                AccessDeniedException accessDeniedException = assertInstanceOf(AccessDeniedException.class, exception);
                assertEquals(expectedFile, accessDeniedException.getFile());
                verify(getExceptionFactory()).createNewInputStreamException(eq(expectedFile), any(SftpException.class));
            }
        }

        @Test
        void testSFTPFailure() {
            // failure: file not found

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> newInputStream(provider, path));
            assertEquals("/foo/bar", exception.getFile());

            verify(getExceptionFactory()).createNewInputStreamException(eq("/foo/bar"), any(SftpException.class));
        }

        private void newInputStream(SFTPFileSystemProvider provider, SFTPPath path, OpenOption... options) throws IOException {
            try (InputStream in = provider.newInputStream(path, options)) {
                in.read();
            }
        }
    }

    @Nested
    class NewOutputStream {

        @Test
        void testExisting() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");

            OpenOption[] options = { StandardOpenOption.WRITE };
            try (OutputStream output = provider().newOutputStream(createPath("/foo/bar"), options)) {
                // don't do anything with the stream, there's a separate test for that
            }
            // verify that the file system can be used after closing the stream
            provider().checkAccess(createPath("/foo/bar"));

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
        }

        @Test
        void testExistingDeleteOnClose() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");

            OpenOption[] options = { StandardOpenOption.DELETE_ON_CLOSE };
            try (OutputStream output = provider().newOutputStream(createPath("/foo/bar"), options)) {
                // don't do anything with the stream, there's a separate test for that
                assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            }
            // verify that the file system can be used after closing the stream
            provider().checkAccess(createPath("/foo"));

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertFalse(Files.exists(getPath("/foo/bar")));
            assertEquals(0, getChildCount("/foo"));
        }

        @Test
        void testExistingCreate() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");

            OpenOption[] options = { StandardOpenOption.CREATE };
            try (OutputStream output = provider().newOutputStream(createPath("/foo/bar"), options)) {
                // don't do anything with the stream, there's a separate test for that
            }
            // verify that the file system can be used after closing the stream
            provider().checkAccess(createPath("/foo/bar"));

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
        }

        @Test
        void testExistingCreateDeleteOnClose() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");

            OpenOption[] options = { StandardOpenOption.CREATE, StandardOpenOption.DELETE_ON_CLOSE };
            try (OutputStream output = provider().newOutputStream(createPath("/foo/bar"), options)) {
                // don't do anything with the stream, there's a separate test for that
                assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            }
            // verify that the file system can be used after closing the stream
            provider().checkAccess(createPath("/foo"));

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertFalse(Files.exists(getPath("/foo/bar")));
        }

        @Test
        void testExistingCreateNew() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");
            OpenOption[] options = { StandardOpenOption.CREATE_NEW };

            FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> newOutputStream(provider, path, options));
            assertEquals("/foo/bar", exception.getFile());

            // verify that the file system can be used after closing the stream
            assertDoesNotThrow(() -> provider().checkAccess(createPath("/foo/bar")));
            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
        }

        @Test
        void testExistingSFTPFailure() throws IOException {
            addDirectory("/foo");
            Path bar = addFile("/foo/bar");
            bar.toFile().setReadOnly();

            // failure: no permission to write

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");
            OpenOption[] options = { StandardOpenOption.WRITE };

            FileSystemException exception = assertThrows(FileSystemException.class, () -> newOutputStream(provider, path, options));
            assertEquals("/foo/bar", exception.getFile());

            verify(getExceptionFactory()).createNewOutputStreamException(eq("/foo/bar"), any(SftpException.class), anyCollection());
            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
        }

        @Test
        void testExistingSFTPFailureDeleteOnClose() throws IOException {
            addDirectory("/foo");
            Path bar = addFile("/foo/bar");
            bar.toFile().setReadOnly();

            // failure: no permission to write

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");
            OpenOption[] options = { StandardOpenOption.DELETE_ON_CLOSE };

            FileSystemException exception = assertThrows(FileSystemException.class, () -> newOutputStream(provider, path, options));
            assertEquals("/foo/bar", exception.getFile());

            verify(getExceptionFactory()).createNewOutputStreamException(eq("/foo/bar"), any(SftpException.class), anyCollection());
            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
        }

        @Test
        void testNonExistingNoCreate() throws IOException {
            addDirectory("/foo");

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");
            OpenOption[] options = { StandardOpenOption.WRITE };

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> newOutputStream(provider, path, options));
            assertEquals("/foo/bar", exception.getFile());

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertFalse(Files.exists(getPath("/foo/bar")));
        }

        @Test
        void testNonExistingCreate() throws IOException {
            addDirectory("/foo");

            OpenOption[] options = { StandardOpenOption.CREATE };
            try (OutputStream input = provider().newOutputStream(createPath("/foo/bar"), options)) {
                // don't do anything with the stream, there's a separate test for that
            } finally {
                assertTrue(Files.isDirectory(getPath("/foo")));
                assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            }
        }

        @Test
        void testNonExistingCreateDeleteOnClose() throws IOException {
            addDirectory("/foo");

            OpenOption[] options = { StandardOpenOption.CREATE, StandardOpenOption.DELETE_ON_CLOSE };
            try (OutputStream input = provider().newOutputStream(createPath("/foo/bar"), options)) {
                // don't do anything with the stream, there's a separate test for that
                // we can't check here that /foo/bar exists, because it will only be stored in the file system once the stream is closed
            } finally {
                assertTrue(Files.isDirectory(getPath("/foo")));
                assertFalse(Files.exists(getPath("/foo/bar")));
                assertEquals(0, getChildCount("/foo"));
            }
        }

        @Test
        void testNonExistingCreateNew() throws IOException {
            addDirectory("/foo");

            OpenOption[] options = { StandardOpenOption.CREATE_NEW };
            try (OutputStream input = provider().newOutputStream(createPath("/foo/bar"), options)) {
                // don't do anything with the stream, there's a separate test for that
            } finally {
                assertTrue(Files.isDirectory(getPath("/foo")));
                assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            }
        }

        @Test
        void testNonExistingCreateNewDeleteOnClose() throws IOException {
            addDirectory("/foo");

            OpenOption[] options = { StandardOpenOption.CREATE_NEW, StandardOpenOption.DELETE_ON_CLOSE };
            try (OutputStream input = provider().newOutputStream(createPath("/foo/bar"), options)) {
                // don't do anything with the stream, there's a separate test for that
                // we can't check here that /foo/bar exists, because it will only be stored in the file system once the stream is closed
            } finally {
                assertTrue(Files.isDirectory(getPath("/foo")));
                assertFalse(Files.exists(getPath("/foo/bar")));
            }
        }

        @Test
        void testDirectoryNoCreate() throws IOException {
            testDirectoryNoCreate("/foo", "/foo");
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testCurrentDirectoryNoCreate(String dir) throws IOException {
            testDirectoryNoCreate(dir, getDefaultDir());
        }

        private void testDirectoryNoCreate(String dir, String expectedFile) throws IOException {
            addDirectoryIfNotExists(dir);

            int oldChildCount = getChildCount(dir);

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath(dir);
            OpenOption[] options = { StandardOpenOption.WRITE };

            FileSystemException exception = assertThrows(FileSystemException.class, () -> newOutputStream(provider, path, options));
            assertEquals(expectedFile, exception.getFile());
            assertEquals(Messages.fileSystemProvider().isDirectory(dir).getReason(), exception.getReason());

            verify(getExceptionFactory(), never()).createNewOutputStreamException(anyString(), any(SftpException.class), anyCollection());
            assertTrue(Files.isDirectory(getPath(dir)));
            assertEquals(oldChildCount, getChildCount(dir));
        }

        @Test
        void testDirectoryDeleteOnClose() throws IOException {
            testDirectoryDeleteOnClose("/foo", "/foo");
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testCurrentDirectoryDeleteOnClose(String dir) throws IOException {
            testDirectoryDeleteOnClose(dir, getDefaultDir());
        }

        private void testDirectoryDeleteOnClose(String dir, String expectedFile) throws IOException {
            addDirectoryIfNotExists(dir);

            int oldChildCount = getChildCount(dir);

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath(dir);
            OpenOption[] options = { StandardOpenOption.DELETE_ON_CLOSE };

            FileSystemException exception = assertThrows(FileSystemException.class, () -> newOutputStream(provider, path, options));
            assertEquals(expectedFile, exception.getFile());
            assertEquals(Messages.fileSystemProvider().isDirectory(dir).getReason(), exception.getReason());

            verify(getExceptionFactory(), never()).createNewOutputStreamException(anyString(), any(SftpException.class), anyCollection());
            assertTrue(Files.isDirectory(getPath(dir)));
            assertEquals(oldChildCount, getChildCount(dir));
        }

        private void newOutputStream(SFTPFileSystemProvider provider, SFTPPath path, OpenOption... options) throws IOException {
            try (OutputStream out = provider.newOutputStream(path, options)) {
                out.write(0);
            }
        }
    }

    @Nested
    class NewByteChannel {

        @Test
        void testRead() throws IOException {
            addDirectory("/foo");
            Path bar = addFile("/foo/bar");
            setContents(bar, new byte[1024]);

            Set<? extends OpenOption> options = EnumSet.noneOf(StandardOpenOption.class);
            try (SeekableByteChannel channel = provider().newByteChannel(createPath("/foo/bar"), options)) {
                // don't do anything with the channel, there's a separate test for that
                assertEquals(Files.size(bar), channel.size());
            }
            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
        }

        @Test
        void testReadNonExisting() {

            // failure: file does not exist

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");
            Set<? extends OpenOption> options = EnumSet.noneOf(StandardOpenOption.class);

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> newByteChannel(provider, path, options));
            assertEquals("/foo/bar", exception.getFile());

            verify(getExceptionFactory()).createNewInputStreamException(eq("/foo/bar"), any(SftpException.class));
            assertFalse(Files.exists(getPath("/foo")));
            assertFalse(Files.exists(getPath("/foo/bar")));
        }

        @Test
        void testWrite() throws IOException {
            Path bar = addFile("/foo/bar");

            byte[] newContents = "Lorem ipsum".getBytes();

            Set<? extends OpenOption> options = EnumSet.of(StandardOpenOption.WRITE);
            try (SeekableByteChannel channel = provider().newByteChannel(createPath("/foo/bar"), options)) {
                // don't do anything with the channel, there's a separate test for that
                assertEquals(0, channel.size());
                channel.write(ByteBuffer.wrap(newContents));
                assertEquals(newContents.length, channel.size());
            }

            assertArrayEquals(newContents, Files.readAllBytes(bar));
        }

        @Test
        void testWriteAppend() throws IOException {
            Path bar = addFile("/foo/bar");
            setContents(bar, new byte[1024]);

            byte[] newContents = "Lorem ipsum".getBytes();

            Set<? extends OpenOption> options = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            try (SeekableByteChannel channel = provider().newByteChannel(createPath("/foo/bar"), options)) {
                // don't do anything with the channel, there's a separate test for that
                long size = Files.size(bar);
                assertEquals(size, channel.size());
                channel.write(ByteBuffer.wrap(newContents));
                assertEquals(size + newContents.length, channel.size());
            }

            byte[] totalNewContents = new byte[1024 + newContents.length];
            System.arraycopy(newContents, 0, totalNewContents, 1024, newContents.length);

            assertArrayEquals(totalNewContents, Files.readAllBytes(bar));
        }

        @Test
        void testCreateWriteExisting() throws IOException {
            Path bar = addFile("/foo/bar");

            byte[] newContents = "Lorem ipsum".getBytes();

            Set<? extends OpenOption> options = EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            try (SeekableByteChannel channel = provider().newByteChannel(createPath("/foo/bar"), options)) {
                // don't do anything with the channel, there's a separate test for that
                assertEquals(0, channel.size());
                channel.write(ByteBuffer.wrap(newContents));
                assertEquals(newContents.length, channel.size());
            }

            assertArrayEquals(newContents, Files.readAllBytes(bar));
        }

        @Test
        void testCreateAppendExisting() throws IOException {
            Path bar = addFile("/foo/bar");
            setContents(bar, new byte[1024]);

            byte[] newContents = "Lorem ipsum".getBytes();

            Set<? extends OpenOption> options = EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            try (SeekableByteChannel channel = provider().newByteChannel(createPath("/foo/bar"), options)) {
                // don't do anything with the channel, there's a separate test for that
                long size = Files.size(bar);
                assertEquals(size, channel.size());
                channel.write(ByteBuffer.wrap(newContents));
                assertEquals(size + newContents.length, channel.size());
            }

            byte[] totalNewContents = new byte[1024 + newContents.length];
            System.arraycopy(newContents, 0, totalNewContents, 1024, newContents.length);

            assertArrayEquals(totalNewContents, Files.readAllBytes(bar));
        }

        @Test
        void testCreateWriteNonExisting() throws IOException {
            addDirectory("/foo");

            byte[] newContents = "Lorem ipsum".getBytes();

            Set<? extends OpenOption> options = EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            try (SeekableByteChannel channel = provider().newByteChannel(createPath("/foo/bar"), options)) {
                // don't do anything with the channel, there's a separate test for that
                assertEquals(0, channel.size());
                channel.write(ByteBuffer.wrap(newContents));
                assertEquals(newContents.length, channel.size());
            }

            Path bar = getFile("/foo/bar");

            assertArrayEquals(newContents, Files.readAllBytes(bar));
        }

        @Test
        void testCreateAppendNonExisting() throws IOException {
            addDirectory("/foo");

            byte[] newContents = "Lorem ipsum".getBytes();

            Set<? extends OpenOption> options = EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            try (SeekableByteChannel channel = provider().newByteChannel(createPath("/foo/bar"), options)) {
                // don't do anything with the channel, there's a separate test for that
                assertEquals(0, channel.size());
                channel.write(ByteBuffer.wrap(newContents));
                assertEquals(newContents.length, channel.size());
            }

            Path bar = getFile("/foo/bar");

            assertArrayEquals(newContents, Files.readAllBytes(bar));
        }

        @Test
        void testCreateNewWriteExisting() throws IOException {
            Path bar = addFile("/foo/bar");
            byte[] oldContents = Files.readAllBytes(bar);

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");
            Set<? extends OpenOption> options = EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

            FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> newByteChannel(provider, path, options));
            assertEquals("/foo/bar", exception.getFile());

            assertArrayEquals(oldContents, Files.readAllBytes(bar));
        }

        @Test
        void testCreateNewAppendExisting() throws IOException {
            Path bar = addFile("/foo/bar");
            setContents(bar, new byte[1024]);

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");
            Set<? extends OpenOption> options = EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND);

            FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> newByteChannel(provider, path, options));
            assertEquals("/foo/bar", exception.getFile());

            assertArrayEquals(new byte[1024], Files.readAllBytes(bar));
        }

        @Test
        void testCreateNewWriteNonExisting() throws IOException {
            addDirectory("/foo");

            byte[] newContents = "Lorem ipsum".getBytes();

            Set<? extends OpenOption> options = EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            try (SeekableByteChannel channel = provider().newByteChannel(createPath("/foo/bar"), options)) {
                // don't do anything with the channel, there's a separate test for that
                assertEquals(0, channel.size());
                channel.write(ByteBuffer.wrap(newContents));
                assertEquals(newContents.length, channel.size());
            }

            Path bar = getFile("/foo/bar");

            assertArrayEquals(newContents, Files.readAllBytes(bar));
        }

        @Test
        void testCreateNewAppendNonExisting() throws IOException {
            addDirectory("/foo");

            byte[] newContents = "Lorem ipsum".getBytes();

            Set<? extends OpenOption> options = EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND);
            try (SeekableByteChannel channel = provider().newByteChannel(createPath("/foo/bar"), options)) {
                // don't do anything with the channel, there's a separate test for that
                assertEquals(0, channel.size());
                channel.write(ByteBuffer.wrap(newContents));
                assertEquals(newContents.length, channel.size());
            }

            Path bar = getFile("/foo/bar");

            assertArrayEquals(newContents, Files.readAllBytes(bar));
        }

        @Test
        void testDirectory() {
            testDirectory("/home", "/home");
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testCurrentDirectory(String dir) {
            testDirectory(dir, getDefaultDir());
        }

        private void testDirectory(String dir, String expectedFile) {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath(dir);

            Set<? extends OpenOption> options = EnumSet.noneOf(StandardOpenOption.class);
            // On Windows this will throw an AccessDeniedException when calling provider.newByteChannel
            // On Linux however that call succeeds, but the backing InputStream is broken
            IOException exception = assertThrows(IOException.class, () -> newByteChannel(provider, path, options));
            if (OS.current() == OS.WINDOWS) {
                AccessDeniedException accessDeniedException = assertInstanceOf(AccessDeniedException.class, exception);
                assertEquals(expectedFile, accessDeniedException.getFile());
                verify(getExceptionFactory()).createNewInputStreamException(eq(expectedFile), any(SftpException.class));
            }
        }

        private void newByteChannel(SFTPFileSystemProvider provider, SFTPPath path, Set<? extends OpenOption> options) throws IOException {
            try (SeekableByteChannel channel = provider.newByteChannel(path, options)) {
                channel.read(ByteBuffer.allocate(1));
            }
        }
    }

    @Nested
    class NewDirectoryStream {

        @ParameterizedTest
        @ValueSource(strings = { "/", CURRENT_DIR })
        @EmptySource
        void testSuccess(String dir) throws IOException {
            try (DirectoryStream<Path> stream = provider().newDirectoryStream(createPath(dir), entry -> true)) {
                assertNotNull(stream);
                // don't do anything with the stream, there's a separate test for that
            }
        }

        @Test
        void testNotExisting() {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.newDirectoryStream(path, entry -> true));
            assertEquals("/foo", exception.getFile());
        }

        @Test
        void testNotDirectory() throws IOException {
            addFile("/foo");

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            NotDirectoryException exception = assertThrows(NotDirectoryException.class, () -> provider.newDirectoryStream(path, entry -> true));
            assertEquals("/foo", exception.getFile());
        }
    }

    @Nested
    class CreateDirectory {

        @Test
        void testSuccess() throws IOException {
            assertFalse(Files.exists(getPath("/foo")));

            provider().createDirectory(createPath("/foo"));

            assertTrue(Files.isDirectory(getPath("/foo")));
        }

        @Test
        void testAlreadyExists() throws IOException {
            addDirectory("/foo/bar");

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");

            FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.createDirectory(path));
            assertEquals("/foo/bar", exception.getFile());

            verify(getExceptionFactory(), never()).createCreateDirectoryException(anyString(), any(SftpException.class));
            assertTrue(Files.exists(getPath("/foo")));
            assertTrue(Files.exists(getPath("/foo/bar")));
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testCurrentDirectory(String dir) {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath(dir);

            FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.createDirectory(path));
            assertEquals(getDefaultDir(), exception.getFile());

            verify(getExceptionFactory(), never()).createCreateDirectoryException(anyString(), any(SftpException.class));
            assertTrue(Files.exists(getPath(dir)));
        }

        @Test
        void testSFTPFailure() {
            // failure: parent does not exist

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.createDirectory(path));
            assertEquals("/foo/bar", exception.getFile());

            verify(getExceptionFactory()).createCreateDirectoryException(eq("/foo/bar"), any(SftpException.class));
            assertFalse(Files.exists(getPath("/foo")));
            assertFalse(Files.exists(getPath("/foo/bar")));
        }
    }

    @Nested
    class Delete {

        @Test
        void testNonExisting() {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.delete(path));
            assertEquals("/foo", exception.getFile());

            verify(getExceptionFactory(), never()).createDeleteException(eq("/foo"), any(SftpException.class), anyBoolean());
        }

        @Test
        void testRoot() {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/");

            FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.delete(path));
            assertEquals("/", exception.getFile());

            verify(getExceptionFactory()).createDeleteException(eq("/"), any(SftpException.class), eq(true));
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testCurrentDirectory(String dir) throws IOException {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath(dir);

            assertTrue(Files.exists(getPath(getDefaultDir())));

            provider.delete(path);

            assertFalse(Files.exists(getPath(getDefaultDir())));
        }

        @Test
        void testFile() throws IOException {
            addFile("/foo/bar");

            provider().delete(createPath("/foo/bar"));

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertFalse(Files.exists(getPath("/foo/bar")));
        }

        @Test
        void testEmptyDir() throws IOException {
            addDirectory("/foo/bar");

            provider().delete(createPath("/foo/bar"));

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertFalse(Files.exists(getPath("/foo/bar")));
        }

        @Test
        void testSFTPFailure() throws IOException {
            addDirectory("/foo/bar/baz");

            // failure: non-empty directory

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");

            FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.delete(path));
            assertEquals("/foo/bar", exception.getFile());

            verify(getExceptionFactory()).createDeleteException(eq("/foo/bar"), any(SftpException.class), eq(true));
            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertTrue(Files.isDirectory(getPath("/foo/bar/baz")));
        }
    }

    @Nested
    class ReadSymbolicLink {

        @Test
        void testToFile() throws IOException {
            Path foo = addFile("/foo");
            addSymLink("/bar", foo);

            Path link = provider().readSymbolicLink(createPath("/bar"));
            assertEquals(createPath("/foo"), link);
        }

        @Test
        void testToDirectory() throws IOException {
            Path foo = addDirectory("/foo");
            addSymLink("/bar", foo);

            Path link = provider().readSymbolicLink(createPath("/bar"));
            assertEquals(createPath("/foo"), link);
        }

        @Test
        void testToNonExistingTarget() throws IOException {
            addSymLink("/bar", getPath("/foo"));

            Path link = provider().readSymbolicLink(createPath("/bar"));
            assertEquals(createPath("/foo"), link);
        }

        @Test
        void testNotExisting() {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.readSymbolicLink(path));
            assertEquals("/foo", exception.getFile());

            verify(getExceptionFactory()).createReadLinkException(eq("/foo"), any(SftpException.class));
        }

        @Test
        void testNoLinkButFile() throws IOException {
            addFile("/foo");

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.readSymbolicLink(path));
            assertEquals("/foo", exception.getFile());

            verify(getExceptionFactory()).createReadLinkException(eq("/foo"), any(SftpException.class));
        }

        @Test
        void testNoLinkButDirectory() throws IOException {
            testNoLinkButDirectory("/foo", "/foo");
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testNoLinkButCurrentDirectory(String dir) throws IOException {
            testNoLinkButDirectory(dir, getDefaultDir());
        }

        private void testNoLinkButDirectory(String dir, String expectedFile) throws IOException {
            addDirectoryIfNotExists(dir);

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath(dir);

            FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.readSymbolicLink(path));
            assertEquals(expectedFile, exception.getFile());

            verify(getExceptionFactory()).createReadLinkException(eq(expectedFile), any(SftpException.class));
        }
    }

    @Nested
    class Copy {

        @Test
        void testSame() throws IOException {
            addDirectory("/home/foo");
            addDirectory("/home/foo/bar");

            CopyOption[] options = {};
            provider().copy(createPath("/home"), createPath(""), options);
            provider().copy(createPath("/home/foo"), createPath("foo"), options);
            provider().copy(createPath("/home/foo/bar"), createPath("foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/home/foo")));
            assertTrue(Files.isDirectory(getPath("/home/foo/bar")));
            assertEquals(0, getChildCount("/home/foo/bar"));
        }

        @Test
        void testNonExisting() throws IOException {
            addDirectory("/foo");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/foo/bar");
            SFTPPath target = createPath("/foo/baz");
            CopyOption[] options = {};

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.copy(source, target, options));
            assertEquals("/foo/bar", exception.getFile());

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertEquals(0, getChildCount("/foo"));
        }

        @Test
        void testSFTPFailure() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");

            // failure: target parent does not exist

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/foo/bar");
            SFTPPath target = createPath("/baz/bar");
            CopyOption[] options = {};

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.copy(source, target, options));
            assertEquals("/baz/bar", exception.getFile());

            verify(getExceptionFactory()).createNewOutputStreamException(eq("/baz/bar"), any(SftpException.class), anyCollection());
            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertFalse(Files.exists(getPath("/baz")));
            assertFalse(Files.exists(getPath("/baz/bar")));
        }

        @Test
        void testRoot() throws IOException {
            // copying a directory (including the root) will not copy its contents, so copying the root is allowed
            addDirectory("/foo");

            CopyOption[] options = {};
            provider().copy(createPath("/"), createPath("/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertEquals(0, getChildCount("/foo/bar"));
        }

        @Test
        void testReplaceFile() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");
            addFile("/baz");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath("/foo/bar");
            CopyOption[] options = {};

            FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.copy(source, target, options));
            assertEquals("/foo/bar", exception.getFile());

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testReplaceFileAllowed() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");
            addFile("/baz");

            CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
            provider().copy(createPath("/baz"), createPath("/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
        }

        @Test
        void testReplaceNonEmptyDir() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");
            addFile("/baz");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath("/foo");
            CopyOption[] options = {};

            FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.copy(source, target, options));
            assertEquals("/foo", exception.getFile());

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testReplaceNonEmptyDirAllowed() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");
            addFile("/baz");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath("/foo");
            CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };

            FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.copy(source, target, options));
            assertEquals("/foo", exception.getFile());

            verify(getExceptionFactory()).createDeleteException(eq("/foo"), any(SftpException.class), eq(true));
            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testReplaceEmptyDir() throws IOException {
            addDirectory("/foo");
            addFile("/baz");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath("/foo");
            CopyOption[] options = {};

            FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.copy(source, target, options));
            assertEquals("/foo", exception.getFile());

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testReplaceEmptyDirAllowed() throws IOException {
            addDirectory("/foo");
            addDirectory("/baz");

            CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
            provider().copy(createPath("/baz"), createPath("/foo"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo")));
        }

        @Test
        void testFile() throws IOException {
            addDirectory("/foo");
            addFile("/baz");

            CopyOption[] options = {};
            provider().copy(createPath("/baz"), createPath("/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testFileMultipleConnections() throws IOException {
            addDirectory("/foo");
            addFile("/baz");

            CopyOption[] options = {};
            fileSystem2.copy(createPath(fileSystem2, "/baz"), createPath(fileSystem2, "/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testEmptyDir() throws IOException {
            addDirectory("/foo");
            addDirectory("/baz");

            CopyOption[] options = {};
            provider().copy(createPath("/baz"), createPath("/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertTrue(Files.isDirectory(getPath("/baz")));

            assertEquals(0, getChildCount("/foo/bar"));
        }

        @Test
        void testNonEmptyDir() throws IOException {
            addDirectory("/foo");
            addDirectory("/baz");
            addFile("/baz/qux");

            CopyOption[] options = {};
            provider().copy(createPath("/baz"), createPath("/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertTrue(Files.isDirectory(getPath("/baz")));

            assertEquals(0, getChildCount("/foo/bar"));
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testCopyCurrentDir(String dir) throws IOException {
            addDirectory("/foo");

            CopyOption[] options = {};
            provider().copy(createPath(dir), createPath("/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertTrue(Files.isDirectory(getPath(getDefaultDir())));

            assertEquals(getChildCount(getDefaultDir()), getChildCount("/foo/bar"));
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testCopyToCurrentDir(String dir) throws IOException {
            addDirectory("/baz");
            addFile("/baz/qux");

            int oldChildCount = getChildCount(getDefaultDir());

            CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
            provider().copy(createPath("/baz"), createPath(dir), options);

            assertTrue(Files.isDirectory(getPath(getDefaultDir())));
            assertTrue(Files.isDirectory(getPath("/baz")));

            assertEquals(oldChildCount, getChildCount(getDefaultDir()));
        }

        @Test
        void testReplaceFileDifferentFileSystems() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");
            addFile("/baz");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath(fileSystem2, "/foo/bar");
            CopyOption[] options = {};

            FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.copy(source, target, options));
            assertEquals("/foo/bar", exception.getFile());

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testReplaceFileAllowedDifferentFileSystems() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");
            addFile("/baz");

            CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
            provider().copy(createPath("/baz"), createPath(fileSystem2, "/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
        }

        @Test
        void testReplaceNonEmptyDirDifferentFileSystems() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");
            addFile("/baz");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath(fileSystem2, "/foo");
            CopyOption[] options = {};

            FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.copy(source, target, options));
            assertEquals("/foo", exception.getFile());

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testReplaceNonEmptyDirAllowedDifferentFileSystems() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");
            addFile("/baz");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath(fileSystem2, "/foo");
            CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };

            FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.copy(source, target, options));
            assertEquals("/foo", exception.getFile());

            verify(getExceptionFactory()).createDeleteException(eq("/foo"), any(SftpException.class), eq(true));
            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testReplaceEmptyDirDifferentFileSystems() throws IOException {
            addDirectory("/foo");
            addFile("/baz");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath(fileSystem2, "/foo");
            CopyOption[] options = {};

            FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.copy(source, target, options));
            assertEquals("/foo", exception.getFile());

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testReplaceEmptyDirAllowedDifferentFileSystems() throws IOException {
            addDirectory("/foo");
            addDirectory("/baz");

            CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
            provider().copy(createPath("/baz"), createPath(fileSystem2, "/foo"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo")));
        }

        @Test
        void testFileDifferentFileSystems() throws IOException {
            addDirectory("/foo");
            addFile("/baz");

            CopyOption[] options = {};
            provider().copy(createPath("/baz"), createPath(fileSystem2, "/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testEmptyDirDifferentFileSystems() throws IOException {
            addDirectory("/foo");
            addDirectory("/baz");

            CopyOption[] options = {};
            provider().copy(createPath("/baz"), createPath(fileSystem2, "/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertTrue(Files.isDirectory(getPath("/baz")));

            assertEquals(0, getChildCount("/foo/bar"));
        }

        @Test
        void testNonEmptyDirDifferentFileSystems() throws IOException {
            addDirectory("/foo");
            addDirectory("/baz");
            addFile("/baz/qux");

            CopyOption[] options = {};
            provider().copy(createPath("/baz"), createPath(fileSystem2, "/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertTrue(Files.isDirectory(getPath("/baz")));

            assertEquals(0, getChildCount("/foo/bar"));
        }

        @Test
        void testWithAttributes() throws IOException {
            addDirectory("/foo");
            addDirectory("/baz");
            addFile("/baz/qux");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath("/foo/bar");
            CopyOption[] options = { StandardCopyOption.COPY_ATTRIBUTES };

            UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class, () -> provider.copy(source, target, options));
            assertChainEquals(Messages.fileSystemProvider().unsupportedCopyOption(StandardCopyOption.COPY_ATTRIBUTES), exception);
        }
    }

    @Nested
    class Move {

        @Test
        void testSame() throws IOException {
            Path foo = addDirectory("/home/foo");
            addDirectory("/home/foo/bar");
            addSymLink("/baz", foo);

            CopyOption[] options = {};
            provider().move(createPath("/"), createPath("/"), options);
            provider().move(createPath("/home"), createPath(""), options);
            provider().move(createPath("/home/foo"), createPath("foo"), options);
            provider().move(createPath("/home/foo/bar"), createPath("foo/bar"), options);
            provider().move(createPath("/home/foo"), createPath("/baz"), options);
            provider().move(createPath("/baz"), createPath("/home/foo"), options);

            assertTrue(Files.isDirectory(getPath("/home/foo")));
            assertTrue(Files.isDirectory(getPath("/home/foo/bar")));
            assertTrue(Files.isSymbolicLink(getPath("/baz")));
            assertEquals(0, getChildCount("/home/foo/bar"));
        }

        @Test
        void testNonExisting() throws IOException {
            addDirectory("/foo");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/foo/bar");
            SFTPPath target = createPath("/foo/baz");
            CopyOption[] options = {};

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.move(source, target, options));
            assertEquals("/foo/bar", exception.getFile());

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertEquals(0, getChildCount("/foo"));
        }

        @Test
        void testSFTPFailure() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");

            // failure: non-existing target parent

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/foo/bar");
            SFTPPath target = createPath("/baz/bar");
            CopyOption[] options = {};

            FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.move(source, target, options));
            assertEquals("/foo/bar", exception.getFile());
            assertEquals("/baz/bar", exception.getOtherFile());

            verify(getExceptionFactory()).createMoveException(eq("/foo/bar"), eq("/baz/bar"), any(SftpException.class));
            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertFalse(Files.exists(getPath("/baz")));
        }

        @Test
        void testEmptyRoot() {

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/");
            SFTPPath target = createPath("/baz");
            CopyOption[] options = {};

            DirectoryNotEmptyException exception = assertThrows(DirectoryNotEmptyException.class, () -> provider.move(source, target, options));
            assertEquals("/", exception.getFile());

            assertFalse(Files.exists(getPath("/baz")));
        }

        @Test
        void testNonEmptyRoot() throws IOException {
            addDirectory("/foo");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/");
            SFTPPath target = createPath("/baz");
            CopyOption[] options = {};

            DirectoryNotEmptyException exception = assertThrows(DirectoryNotEmptyException.class, () -> provider.move(source, target, options));
            assertEquals("/", exception.getFile());

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertFalse(Files.exists(getPath("/baz")));
        }

        @Test
        void testReplaceFile() throws IOException {
            addDirectory("/foo");
            addDirectory("/foo/bar");
            addFile("/baz");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath("/foo/bar");
            CopyOption[] options = {};

            FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.move(source, target, options));
            assertEquals("/baz", exception.getFile());
            assertEquals("/foo/bar", exception.getOtherFile());

            verify(getExceptionFactory()).createMoveException(eq("/baz"), eq("/foo/bar"), any(SftpException.class));
            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testReplaceFileAllowed() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");
            addFile("/baz");

            CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
            provider().move(createPath("/baz"), createPath("/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertFalse(Files.exists(getPath("/baz")));
        }

        @Test
        void testReplaceEmptyDir() throws IOException {
            addDirectory("/foo");
            addFile("/baz");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath("/foo");
            CopyOption[] options = {};

            FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.move(source, target, options));
            assertEquals("/baz", exception.getFile());
            assertEquals("/foo", exception.getOtherFile());

            verify(getExceptionFactory()).createMoveException(eq("/baz"), eq("/foo"), any(SftpException.class));
            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testReplaceEmptyDirAllowed() throws IOException {
            addDirectory("/foo");
            addFile("/baz");

            CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
            provider().move(createPath("/baz"), createPath("/foo"), options);

            assertTrue(Files.isRegularFile(getPath("/foo")));
            assertFalse(Files.exists(getPath("/baz")));
        }

        @Test
        void testFile() throws IOException {
            addDirectory("/foo");
            addFile("/baz");

            CopyOption[] options = {};
            provider().move(createPath("/baz"), createPath("/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertFalse(Files.exists(getPath("/baz")));
        }

        @Test
        void testEmptyDir() throws IOException {
            addDirectory("/foo");
            addDirectory("/baz");

            CopyOption[] options = {};
            provider().move(createPath("/baz"), createPath("/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertFalse(Files.exists(getPath("/baz")));
        }

        @Test
        void testNonEmptyDir() throws IOException {
            addDirectory("/foo");
            addDirectory("/baz");
            addFile("/baz/qux");

            CopyOption[] options = {};
            provider().move(createPath("/baz"), createPath("/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar/qux")));
            assertEquals(1, getChildCount("/foo"));
            assertEquals(1, getChildCount("/foo/bar"));
        }

        @Test
        void testNonEmptyDirSameParent() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");

            CopyOption[] options = {};
            try {
                provider().move(createPath("/foo"), createPath("/baz"), options);
            } finally {
                assertFalse(Files.exists(getPath("/foo")));
                assertTrue(Files.isDirectory(getPath("/baz")));
                assertTrue(Files.isRegularFile(getPath("/baz/bar")));
            }
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testMoveCurrentDir(String dir) throws IOException {
            addDirectory("/foo");

            int oldChildCount = getChildCount(getDefaultDir());

            CopyOption[] options = {};
            provider().move(createPath(dir), createPath("/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertFalse(Files.isDirectory(getPath(getDefaultDir())));

            assertEquals(oldChildCount, getChildCount("/foo/bar"));
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testMoveToCurrentDir(String dir) throws IOException {
            addDirectory("/baz");
            addFile("/baz/qux");

            CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
            provider().move(createPath("/baz"), createPath(dir), options);

            assertTrue(Files.isDirectory(getPath(getDefaultDir())));
            assertFalse(Files.isDirectory(getPath("/baz")));

            assertEquals(1, getChildCount(getDefaultDir()));
        }

        @Test
        void testReplaceFileDifferentFileSystem() throws IOException {
            addDirectory("/foo");
            addDirectory("/foo/bar");
            addFile("/baz");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath(fileSystem2, "/foo/bar");
            CopyOption[] options = {};

            FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.move(source, target, options));
            assertEquals("/foo/bar", exception.getFile());

            verify(getExceptionFactory(), never()).createMoveException(anyString(), anyString(), any(SftpException.class));
            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        @Test
        void testReplaceFileAllowedDifferentFileSystem() throws IOException {
            addDirectory("/foo");
            addFile("/foo/bar");
            addFile("/baz");

            CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
            provider().move(createPath("/baz"), createPath(fileSystem2, "/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertFalse(Files.exists(getPath("/baz")));
        }

        @Test
        void testReplaceEmptyDirDifferentFileSystem() throws IOException {
            addDirectory("/foo");
            addFile("/baz");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath(fileSystem2, "/foo");
            CopyOption[] options = {};

            FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.move(source, target, options));
            assertEquals("/foo", exception.getFile());

            verify(getExceptionFactory(), never()).createMoveException(anyString(), anyString(), any(SftpException.class));
            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/baz")));
        }

        void testReplaceEmptyDirAllowedDifferentFileSystem() throws IOException {
            addDirectory("/foo");
            addFile("/baz");

            CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
            provider().move(createPath("/baz"), createPath(fileSystem2, "/foo"), options);

            assertTrue(Files.isRegularFile(getPath("/foo")));
            assertFalse(Files.exists(getPath("/baz")));
        }

        @Test
        void testFileDifferentFileSystem() throws IOException {
            addDirectory("/foo");
            addFile("/baz");

            CopyOption[] options = {};
            provider().move(createPath("/baz"), createPath(fileSystem2, "/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isRegularFile(getPath("/foo/bar")));
            assertFalse(Files.exists(getPath("/baz")));
        }

        @Test
        void testEmptyDirDifferentFileSystem() throws IOException {
            addDirectory("/foo");
            addDirectory("/baz");

            CopyOption[] options = {};
            provider().move(createPath("/baz"), createPath(fileSystem2, "/foo/bar"), options);

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertFalse(Files.exists(getPath("/baz")));
        }

        @Test
        void testNonEmptyDirDifferentFileSystem() throws IOException {
            addDirectory("/foo");
            addDirectory("/baz");
            addFile("/baz/qux");

            SFTPFileSystemProvider provider = provider();
            SFTPPath source = createPath("/baz");
            SFTPPath target = createPath(fileSystem2, "/foo/bar");
            CopyOption[] options = {};

            FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.move(source, target, options));
            assertEquals("/baz", exception.getFile());

            verify(getExceptionFactory()).createDeleteException(eq("/baz"), any(SftpException.class), eq(true));
            assertTrue(Files.isDirectory(getPath("/baz")));
            assertTrue(Files.isRegularFile(getPath("/baz/qux")));
            assertEquals(1, getChildCount("/baz"));

            assertTrue(Files.isDirectory(getPath("/foo")));
            assertTrue(Files.isDirectory(getPath("/foo/bar")));
            assertFalse(Files.isRegularFile(getPath("/foo/bar/qux")));
            assertEquals(1, getChildCount("/foo"));
            assertEquals(0, getChildCount("/foo/bar"));
        }
    }

    @Nested
    class IsSameFile {

        @Test
        void testEquals() throws IOException {

            assertTrue(provider().isSameFile(createPath("/"), createPath("/")));
            assertTrue(provider().isSameFile(createPath("/foo"), createPath("/foo")));
            assertTrue(provider().isSameFile(createPath("/foo/bar"), createPath("/foo/bar")));

            assertTrue(provider().isSameFile(createPath(""), createPath("")));
            assertTrue(provider().isSameFile(createPath("foo"), createPath("foo")));
            assertTrue(provider().isSameFile(createPath("foo/bar"), createPath("foo/bar")));

            assertTrue(provider().isSameFile(createPath(""), createPath("/home")));
            assertTrue(provider().isSameFile(createPath("/home"), createPath("")));

            assertTrue(provider().isSameFile(createPath(CURRENT_DIR), createPath("/home")));
            assertTrue(provider().isSameFile(createPath("/home"), createPath(CURRENT_DIR)));

            assertTrue(provider().isSameFile(createPath(""), createPath(CURRENT_DIR)));
            assertTrue(provider().isSameFile(createPath(CURRENT_DIR), createPath("")));
        }

        @Test
        void testExisting() throws IOException {
            Path bar = addFile("/home/foo/bar");
            addSymLink("/bar", bar);

            assertTrue(provider().isSameFile(createPath("/home"), createPath("")));
            assertTrue(provider().isSameFile(createPath("/home/foo"), createPath("foo")));
            assertTrue(provider().isSameFile(createPath("/home/foo/bar"), createPath("foo/bar")));

            assertTrue(provider().isSameFile(createPath(""), createPath("/home")));
            assertTrue(provider().isSameFile(createPath("foo"), createPath("/home/foo")));
            assertTrue(provider().isSameFile(createPath("foo/bar"), createPath("/home/foo/bar")));

            assertFalse(provider().isSameFile(createPath("foo"), createPath("foo/bar")));

            assertTrue(provider().isSameFile(createPath("/bar"), createPath("/home/foo/bar")));
        }

        @Test
        void testFirstNonExisting() {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");
            SFTPPath path2 = createPath("/");

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.isSameFile(path, path2));
            assertEquals("/foo", exception.getFile());
        }

        @Test
        void testSecondNonExisting() {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/");
            SFTPPath path2 = createPath("/foo");

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.isSameFile(path, path2));
            assertEquals("/foo", exception.getFile());
        }
    }

    @Nested
    class IsHidden {

        @Test
        void testSuccess() throws IOException {
            addDirectory("/foo");
            addDirectory("/.foo");
            addFile("/foo/bar");
            addFile("/foo/.bar");

            assertFalse(provider().isHidden(createPath("/foo")));
            assertTrue(provider().isHidden(createPath("/.foo")));
            assertFalse(provider().isHidden(createPath("/foo/bar")));
            assertTrue(provider().isHidden(createPath("/foo/.bar")));
            assertFalse(provider().isHidden(createPath("")));
            assertFalse(provider().isHidden(createPath(CURRENT_DIR)));
        }

        @Test
        void testNonExisting() {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.isHidden(path));
            assertEquals("/foo", exception.getFile());
        }
    }

    @Nested
    class CheckAccess {

        @Test
        void testNonExisting() {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.checkAccess(path));
            assertEquals("/foo/bar", exception.getFile());
        }

        @Test
        void testNoModes() throws IOException {
            addDirectory("/foo/bar");

            provider().checkAccess(createPath("/foo/bar"));
        }

        @Test
        void testOnlyRead() throws IOException {
            addDirectory("/foo/bar");

            provider().checkAccess(createPath("/foo/bar"), AccessMode.READ);
        }

        @Test
        void testOnlyWriteNotReadOnly() throws IOException {
            addDirectory("/foo/bar");

            provider().checkAccess(createPath("/foo/bar"), AccessMode.WRITE);
        }

        @Test
        @DisabledOnOs(value = OS.WINDOWS, disabledReason = "On Windows, the permissions are not reported correctly, but always as rw-rw-rw-")
        void testOnlyWriteReadOnly() throws IOException {
            Path bar = addDirectory("/foo/bar");
            bar.toFile().setReadOnly();

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");

            AccessDeniedException exception = assertThrows(AccessDeniedException.class, () -> provider.checkAccess(path, AccessMode.WRITE));
            assertEquals("/foo/bar", exception.getFile());
        }

        @Test
        void testOnlyExecute() throws IOException {
            Path bar = addFile("/foo/bar");
            bar.toFile().setReadOnly();

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo/bar");

            AccessDeniedException exception = assertThrows(AccessDeniedException.class, () -> provider.checkAccess(path, AccessMode.EXECUTE));
            assertEquals("/foo/bar", exception.getFile());
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testCurrentDirectory(String dir) {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath(dir);

            assertDoesNotThrow(() -> provider.checkAccess(path, AccessMode.READ));
            assertDoesNotThrow(() -> provider.checkAccess(path, AccessMode.WRITE));
        }
    }

    @Nested
    class ReadAttributesObject {

        @Test
        void testFileFollowLinks() throws IOException {
            Path foo = addFile("/foo");
            setContents(foo, new byte[1024]);

            PosixFileAttributes attributes = provider().readAttributes(createPath("/foo"), PosixFileAttributes.class);

            assertEquals(Files.size(foo), attributes.size());
            assertNotNull(attributes.owner().getName());
            assertNotNull(attributes.group().getName());
            assertNotNull(attributes.permissions());
            assertFalse(attributes.isDirectory());
            assertTrue(attributes.isRegularFile());
            assertFalse(attributes.isSymbolicLink());
            assertFalse(attributes.isOther());
        }

        @Test
        void testFileNoFollowLinks() throws IOException {
            Path foo = addFile("/foo");
            setContents(foo, new byte[1024]);

            PosixFileAttributes attributes = provider().readAttributes(createPath("/foo"), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);

            assertEquals(Files.size(foo), attributes.size());
            assertNotNull(attributes.owner().getName());
            assertNotNull(attributes.group().getName());
            assertNotNull(attributes.permissions());
            assertFalse(attributes.isDirectory());
            assertTrue(attributes.isRegularFile());
            assertFalse(attributes.isSymbolicLink());
            assertFalse(attributes.isOther());
        }

        @Test
        void testDirectoryFollowLinks() throws IOException {
            addDirectory("/foo");

            PosixFileAttributes attributes = provider().readAttributes(createPath("/foo"), PosixFileAttributes.class);

            // Directories always have size 0 when using sshd-core
            assertEquals(0, attributes.size());
            assertNotNull(attributes.owner().getName());
            assertNotNull(attributes.group().getName());
            assertNotNull(attributes.permissions());
            assertTrue(attributes.isDirectory());
            assertFalse(attributes.isRegularFile());
            assertFalse(attributes.isSymbolicLink());
            assertFalse(attributes.isOther());
        }

        @Test
        void testDirectoryNoFollowLinks() throws IOException {
            addDirectory("/foo");

            PosixFileAttributes attributes = provider().readAttributes(createPath("/foo"), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);

            // Directories always have size 0 when using sshd-core
            assertEquals(0, attributes.size());
            assertNotNull(attributes.owner().getName());
            assertNotNull(attributes.group().getName());
            assertNotNull(attributes.permissions());
            assertTrue(attributes.isDirectory());
            assertFalse(attributes.isRegularFile());
            assertFalse(attributes.isSymbolicLink());
            assertFalse(attributes.isOther());
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testCurrentDirectory(String dir) throws IOException {
            PosixFileAttributes attributes = provider().readAttributes(createPath(dir), PosixFileAttributes.class);

            // Directories always have size 0 when using sshd-core
            assertEquals(0, attributes.size());
            assertNotNull(attributes.owner().getName());
            assertNotNull(attributes.group().getName());
            assertNotNull(attributes.permissions());
            assertTrue(attributes.isDirectory());
            assertFalse(attributes.isRegularFile());
            assertFalse(attributes.isSymbolicLink());
            assertFalse(attributes.isOther());
        }

        @Test
        void testSymLinkToFileFollowLinks() throws IOException {
            Path foo = addFile("/foo");
            setContents(foo, new byte[1024]);
            Path bar = addSymLink("/bar", foo);

            PosixFileAttributes attributes = provider().readAttributes(createPath("/bar"), PosixFileAttributes.class);

            long sizeOfFoo = Files.readAttributes(foo, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).size();
            long sizeOfBar = Files.readAttributes(bar, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).size();

            assertEquals(sizeOfFoo, attributes.size());
            assertNotEquals(sizeOfBar, attributes.size());
            assertNotNull(attributes.owner().getName());
            assertNotNull(attributes.group().getName());
            assertNotNull(attributes.permissions());
            assertFalse(attributes.isDirectory());
            assertTrue(attributes.isRegularFile());
            assertFalse(attributes.isSymbolicLink());
            assertFalse(attributes.isOther());
        }

        @Test
        void testSymLinkToFileNoFollowLinks() throws IOException {
            Path foo = addFile("/foo");
            setContents(foo, new byte[1024]);
            Path bar = addSymLink("/bar", foo);

            PosixFileAttributes attributes = provider().readAttributes(createPath("/bar"), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);

            long sizeOfFoo = Files.readAttributes(foo, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).size();
            long sizeOfBar = Files.readAttributes(bar, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).size();

            assertEquals(sizeOfBar, attributes.size());
            assertNotEquals(sizeOfFoo, attributes.size());
            assertNotNull(attributes.owner().getName());
            assertNotNull(attributes.group().getName());
            assertNotNull(attributes.permissions());
            assertFalse(attributes.isDirectory());
            assertFalse(attributes.isRegularFile());
            assertTrue(attributes.isSymbolicLink());
            assertFalse(attributes.isOther());
        }

        @Test
        void testSymLinkToDirectoryFollowLinks() throws IOException {
            Path foo = addDirectory("/foo");
            addSymLink("/bar", foo);

            PosixFileAttributes attributes = provider().readAttributes(createPath("/bar"), PosixFileAttributes.class);

            // Directories always have size 0 when using sshd-core
            assertEquals(0, attributes.size());
            assertNotNull(attributes.owner().getName());
            assertNotNull(attributes.group().getName());
            assertNotNull(attributes.permissions());
            assertTrue(attributes.isDirectory());
            assertFalse(attributes.isRegularFile());
            assertFalse(attributes.isSymbolicLink());
            assertFalse(attributes.isOther());
        }

        @Test
        void testSymLinkToDirectoryNoFollowLinks() throws IOException {
            Path foo = addDirectory("/foo");
            Path bar = addSymLink("/bar", foo);

            PosixFileAttributes attributes = provider().readAttributes(createPath("/bar"), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);

            long sizeOfBar = Files.readAttributes(bar, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).size();

            // on Windows, foo and bar have the same sizes
            assertEquals(sizeOfBar, attributes.size());
            assertNotNull(attributes.owner().getName());
            assertNotNull(attributes.group().getName());
            assertNotNull(attributes.permissions());
            assertFalse(attributes.isDirectory());
            assertFalse(attributes.isRegularFile());
            assertTrue(attributes.isSymbolicLink());
            assertFalse(attributes.isOther());
        }

        @Test
        void testNonExisting() {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.readAttributes(path, PosixFileAttributes.class));
            assertEquals("/foo", exception.getFile());
        }

        @Test
        void testReadAttributesUnsupportedType() {
            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");
            Class<? extends BasicFileAttributes> type = DosFileAttributes.class;

            UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class, () -> provider.readAttributes(path, type));
            assertChainEquals(Messages.fileSystemProvider().unsupportedFileAttributesType(type), exception);
        }
    }

    @Nested
    class ReadAttributesMap {

        @ParameterizedTest(name = "{0}")
        @CsvSource({
                "lastModifiedTime, lastModifiedTime",
                "basic:lastModifiedTime, lastModifiedTime",
                "posix:lastModifiedTime, lastModifiedTime",
                "lastAccessTime, lastAccessTime",
                "basic:lastAccessTime, lastAccessTime",
                "posix:lastAccessTime, lastAccessTime",
                "creationTime, creationTime",
                "basic:creationTime, creationTime",
                "posix:creationTime, creationTime"
        })
        void testSingleProperty(String attributeName, String expectedKey) throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
            assertEquals(Collections.singleton(expectedKey), attributes.keySet());
            assertNotNull(attributes.get(expectedKey));
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = { "size", "basic:size", "posix:size" })
        void testSize(String attributeName) throws IOException {
            Path foo = addFile("/foo");
            setContents(foo, new byte[1024]);

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
            Map<String, ?> expected = Collections.singletonMap("size", Files.size(foo));
            assertEquals(expected, attributes);
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = { "isRegularFile", "basic:isRegularFile", "posix:isRegularFile" })
        void testIsRegularFile(String attributeName) throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
            Map<String, ?> expected = Collections.singletonMap("isRegularFile", false);
            assertEquals(expected, attributes);
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = { "isDirectory", "basic:isDirectory", "posix:isDirectory" })
        void testIsDirectory(String attributeName) throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
            Map<String, ?> expected = Collections.singletonMap("isDirectory", true);
            assertEquals(expected, attributes);
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = { "isSymbolicLink", "basic:isSymbolicLink", "posix:isSymbolicLink" })
        void testIsSymbolicLink(String attributeName) throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
            Map<String, ?> expected = Collections.singletonMap("isSymbolicLink", false);
            assertEquals(expected, attributes);
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = { "isOther", "basic:isOther", "posix:isOther" })
        void testIsOther(String attributeName) throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
            Map<String, ?> expected = Collections.singletonMap("isOther", false);
            assertEquals(expected, attributes);
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = { "fileKey", "basic:fileKey", "posix:fileKey" })
        void testFileKey(String attributeName) throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
            Map<String, ?> expected = Collections.singletonMap("fileKey", null);
            assertEquals(expected, attributes);
        }

        @Test
        void testNoTypeMultipleForDirectory() throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "size,isDirectory");
            Map<String, Object> expected = new HashMap<>();
            // Directories always have size 0 when using sshd-core
            expected.put("size", 0L);
            expected.put("isDirectory", true);
            assertEquals(expected, attributes);
        }

        @Test
        void testNoTypeMultipleForFile() throws IOException {
            Path foo = addFile("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "size,isRegularFile");
            Map<String, Object> expected = new HashMap<>();
            expected.put("size", Files.size(foo));
            expected.put("isRegularFile", true);
            assertEquals(expected, attributes);
        }

        @Test
        void testNoTypeAllForDirectory() throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "*");
            Map<String, Object> expected = new HashMap<>();
            // Directories always have size 0 when using sshd-core
            expected.put("size", 0L);
            expected.put("isRegularFile", false);
            expected.put("isDirectory", true);
            expected.put("isSymbolicLink", false);
            expected.put("isOther", false);
            expected.put("fileKey", null);

            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertEquals(expected, attributes);

            attributes = provider().readAttributes(createPath("/foo"), "lastModifiedTime,*");
            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertEquals(expected, attributes);
        }

        @Test
        void testNoTypeAllForFile() throws IOException {
            Path foo = addFile("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "*");
            Map<String, Object> expected = new HashMap<>();
            expected.put("size", Files.size(foo));
            expected.put("isRegularFile", true);
            expected.put("isDirectory", false);
            expected.put("isSymbolicLink", false);
            expected.put("isOther", false);
            expected.put("fileKey", null);

            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertEquals(expected, attributes);

            attributes = provider().readAttributes(createPath("/foo"), "lastModifiedTime,*");
            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertEquals(expected, attributes);
        }

        @Test
        void testBasicMultipleForDirectory() throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "basic:size,isDirectory");
            Map<String, Object> expected = new HashMap<>();
            // Directories always have size 0 when using sshd-core
            expected.put("size", 0L);
            expected.put("isDirectory", true);
            assertEquals(expected, attributes);
        }

        @Test
        void testBasicMultipleForFile() throws IOException {
            Path foo = addFile("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "basic:size,isRegularFile");
            Map<String, Object> expected = new HashMap<>();
            expected.put("size", Files.size(foo));
            expected.put("isRegularFile", true);
            assertEquals(expected, attributes);
        }

        @Test
        void testBasicAllForDirectory() throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "basic:*");
            Map<String, Object> expected = new HashMap<>();
            // Directories always have size 0 when using sshd-core
            expected.put("size", 0L);
            expected.put("isRegularFile", false);
            expected.put("isDirectory", true);
            expected.put("isSymbolicLink", false);
            expected.put("isOther", false);
            expected.put("fileKey", null);

            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertEquals(expected, attributes);

            attributes = provider().readAttributes(createPath("/foo"), "basic:lastModifiedTime,*");
            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertEquals(expected, attributes);
        }

        @Test
        void testBasicAllForFile() throws IOException {
            Path foo = addFile("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "basic:*");
            Map<String, Object> expected = new HashMap<>();
            expected.put("size", Files.size(foo));
            expected.put("isRegularFile", true);
            expected.put("isDirectory", false);
            expected.put("isSymbolicLink", false);
            expected.put("isOther", false);
            expected.put("fileKey", null);

            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertEquals(expected, attributes);

            attributes = provider().readAttributes(createPath("/foo"), "basic:lastModifiedTime,*");
            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertEquals(expected, attributes);
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = { "owner:owner", "posix:owner" })
        void testOwner(String attributeName) throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
            assertEquals(Collections.singleton("owner"), attributes.keySet());
            assertNotNull(attributes.get("owner"));
        }

        @Test
        void testOwnerAll() throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "owner:*");
            assertEquals(Collections.singleton("owner"), attributes.keySet());
            assertNotNull(attributes.get("owner"));

            attributes = provider().readAttributes(createPath("/foo"), "owner:owner,*");
            assertEquals(Collections.singleton("owner"), attributes.keySet());
            assertNotNull(attributes.get("owner"));
        }

        @Test
        void testPosixGroup() throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "posix:group");
            assertEquals(Collections.singleton("group"), attributes.keySet());
            assertNotNull(attributes.get("group"));
        }

        @Test
        void testPosixPermissions() throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "posix:permissions");
            assertEquals(Collections.singleton("permissions"), attributes.keySet());
            assertNotNull(attributes.get("permissions"));
        }

        @Test
        void testPosixMultipleForDirectory() throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "posix:size,owner,group");
            // Directories always have size 0 when using sshd-core
            Map<String, ?> expected = Collections.singletonMap("size", 0L);
            assertNotNull(attributes.remove("owner"));
            assertNotNull(attributes.remove("group"));
            assertEquals(expected, attributes);
        }

        @Test
        void testPosixMultipleForFile() throws IOException {
            Path foo = addFile("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "posix:size,owner,group");
            Map<String, ?> expected = Collections.singletonMap("size", Files.size(foo));
            assertNotNull(attributes.remove("owner"));
            assertNotNull(attributes.remove("group"));
            assertEquals(expected, attributes);
        }

        @Test
        void testPosixAllForDirectory() throws IOException {
            addDirectory("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "posix:*");
            Map<String, Object> expected = new HashMap<>();
            // Directories always have size 0 when using sshd-core
            expected.put("size", 0L);
            expected.put("isRegularFile", false);
            expected.put("isDirectory", true);
            expected.put("isSymbolicLink", false);
            expected.put("isOther", false);
            expected.put("fileKey", null);

            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertNotNull(attributes.remove("owner"));
            assertNotNull(attributes.remove("group"));
            assertNotNull(attributes.remove("permissions"));
            assertEquals(expected, attributes);

            attributes = provider().readAttributes(createPath("/foo"), "posix:lastModifiedTime,*");
            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertNotNull(attributes.remove("owner"));
            assertNotNull(attributes.remove("group"));
            assertNotNull(attributes.remove("permissions"));
            assertEquals(expected, attributes);
        }

        @Test
        void testPosixAllForFile() throws IOException {
            Path foo = addFile("/foo");

            Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "posix:*");
            Map<String, Object> expected = new HashMap<>();
            expected.put("size", Files.size(foo));
            expected.put("isRegularFile", true);
            expected.put("isDirectory", false);
            expected.put("isSymbolicLink", false);
            expected.put("isOther", false);
            expected.put("fileKey", null);

            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertNotNull(attributes.remove("owner"));
            assertNotNull(attributes.remove("group"));
            assertNotNull(attributes.remove("permissions"));
            assertEquals(expected, attributes);

            attributes = provider().readAttributes(createPath("/foo"), "posix:lastModifiedTime,*");
            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertNotNull(attributes.remove("owner"));
            assertNotNull(attributes.remove("group"));
            assertNotNull(attributes.remove("permissions"));
            assertEquals(expected, attributes);
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testCurrentDirectory(String dir) throws IOException {
            Map<String, Object> attributes = provider().readAttributes(createPath(dir), "*");
            Map<String, Object> expected = new HashMap<>();
            // Directories always have size 0 when using sshd-core
            expected.put("size", 0L);
            expected.put("isRegularFile", false);
            expected.put("isDirectory", true);
            expected.put("isSymbolicLink", false);
            expected.put("isOther", false);
            expected.put("fileKey", null);

            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertEquals(expected, attributes);

            attributes = provider().readAttributes(createPath(dir), "lastModifiedTime,*");
            assertNotNull(attributes.remove("lastModifiedTime"));
            assertNotNull(attributes.remove("lastAccessTime"));
            assertNotNull(attributes.remove("creationTime"));
            assertEquals(expected, attributes);
        }

        @Test
        void testUnsupportedAttribute() throws IOException {
            addDirectory("/foo");

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                    () -> provider.readAttributes(path, "posix:lastModifiedTime,owner,dummy"));
            assertChainEquals(Messages.fileSystemProvider().unsupportedFileAttribute("dummy"), exception);
        }

        @ParameterizedTest(name = "{0}")
        @CsvSource({
                "basic:owner, owner",
                "basic:permissions, permissions",
                "basic:group, group",
                "owner:permissions, permissions",
                "owner:group, group",
                "owner:size, size"
        })
        void testSupportedAttributeForWrongView(String attributes, String attribute) throws IOException {
            addDirectory("/foo");

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> provider.readAttributes(path, attributes));
            assertChainEquals(Messages.fileSystemProvider().unsupportedFileAttribute(attribute), exception);
        }

        @Test
        void testUnsupportedView() throws IOException {
            addDirectory("/foo");

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                    () -> provider.readAttributes(path, "zipfs:*"));
            assertChainEquals(Messages.fileSystemProvider().unsupportedFileAttributeView("zipfs"), exception);
        }
    }

    @Test
    void testPrefixAttributes() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("size", 1L);
        attributes.put("isDirectory", "false");
        attributes.put("owner", new SimpleUserPrincipal("test"));

        Map<String, Object> expected = new HashMap<>();
        expected.put("posix:size", 1L);
        expected.put("posix:isDirectory", "false");
        expected.put("posix:owner", new SimpleUserPrincipal("test"));

        assertEquals(expected, SFTPFileSystem.prefixAttributes(attributes, FileAttributeViewMetadata.POSIX));
    }

    @Nested
    class SetOwner {

        @Test
        void testSuccess() throws IOException {
            addDirectory("/foo");

            SFTPPath fooPath = createPath("/foo");

            provider().getFileAttributeView(fooPath, FileOwnerAttributeView.class).setOwner(new SimpleUserPrincipal("1"));

            Map<String, Object> expected = new HashMap<>();
            expected.put("uid", 1);
            expected.put("gid", Integer.valueOf(provider().readAttributes(fooPath, PosixFileAttributes.class).group().getName()));

            assertAttributesModified(fooPath, expected);
        }

        @Test
        void testNonExisting() {
            SFTPPath path = createPath("/foo/bar");
            PosixFileAttributeView fileAttributeView = provider().getFileAttributeView(path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
            UserPrincipal owner = new SimpleUserPrincipal("1");

            FileSystemException exception = assertThrows(FileSystemException.class, () -> fileAttributeView.setOwner(owner));
            assertEquals("/foo/bar", exception.getFile());

            verify(getExceptionFactory()).createSetOwnerException(eq("/foo/bar"), any(SftpException.class));
        }

        @Test
        void testNonNumeric() throws IOException {
            addFile("/foo/bar");

            PosixFileAttributeView fileAttributeView = provider().getFileAttributeView(createPath("/foo/bar"), PosixFileAttributeView.class);
            UserPrincipal owner = new SimpleUserPrincipal("test");

            IOException exception = assertThrows(IOException.class, () -> fileAttributeView.setOwner(owner));
            assertThat(exception.getCause(), instanceOf(NumberFormatException.class));

            verify(getExceptionFactory(), never()).createSetOwnerException(anyString(), any(SftpException.class));
        }
    }

    @Nested
    class SetGroup {

        @Test
        void testSuccess() throws IOException {
            addDirectory("/foo");

            SFTPPath fooPath = createPath("/foo");

            provider().getFileAttributeView(fooPath, PosixFileAttributeView.class).setGroup(new SimpleGroupPrincipal("1"));

            Map<String, Object> expected = new HashMap<>();
            expected.put("uid", Integer.valueOf(provider().readAttributes(fooPath, PosixFileAttributes.class).owner().getName()));
            expected.put("gid", 1);

            assertAttributesModified(fooPath, expected);
        }

        @Test
        void testNonExisting() {
            SFTPPath path = createPath("/foo/bar");
            PosixFileAttributeView fileAttributeView = provider().getFileAttributeView(path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
            GroupPrincipal group = new SimpleGroupPrincipal("1");

            FileSystemException exception = assertThrows(FileSystemException.class, () -> fileAttributeView.setGroup(group));
            assertEquals("/foo/bar", exception.getFile());

            verify(getExceptionFactory()).createSetGroupException(eq("/foo/bar"), any(SftpException.class));
        }

        @Test
        void testNonNumeric() throws IOException {
            addFile("/foo/bar");

            PosixFileAttributeView fileAttributeView = provider().getFileAttributeView(createPath("/foo/bar"), PosixFileAttributeView.class);
            GroupPrincipal group = new SimpleGroupPrincipal("test");

            IOException exception = assertThrows(IOException.class, () -> fileAttributeView.setGroup(group));
            assertThat(exception.getCause(), instanceOf(NumberFormatException.class));

            verify(getExceptionFactory(), never()).createSetOwnerException(anyString(), any(SftpException.class));
        }
    }

    @Nested
    class SetPermissions {

        @Test
        void testSuccess() throws IOException {
            addDirectory("/foo");

            SFTPPath fooPath = createPath("/foo");
            Set<PosixFilePermission> permissions = EnumSet.of(PosixFilePermission.OWNER_READ);

            provider().getFileAttributeView(fooPath, PosixFileAttributeView.class).setPermissions(permissions);

            assertAttributesModified(fooPath, Collections.singletonMap("permissions", permissions));
        }

        @Test
        void testNonExisting() {
            SFTPPath path = createPath("/foo/bar");
            PosixFileAttributeView fileAttributeView = provider().getFileAttributeView(path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
            Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("r--r--r--");

            FileSystemException exception = assertThrows(FileSystemException.class, () -> fileAttributeView.setPermissions(permissions));
            assertEquals("/foo/bar", exception.getFile());

            verify(getExceptionFactory()).createSetPermissionsException(eq("/foo/bar"), any(SftpException.class));
        }
    }

    @Nested
    class SetLastModifiedTime {

        @Test
        void testExisting() throws IOException {
            addFile("/foo/bar");

            SFTPPath path = createPath("/foo/bar");
            // times are in seconds
            FileTime lastModifiedTime = FileTime.from(123456789L, TimeUnit.SECONDS);
            provider().getFileAttributeView(path, BasicFileAttributeView.class).setTimes(lastModifiedTime, null, null);
            assertEquals(lastModifiedTime, provider().readAttributes(path, BasicFileAttributes.class).lastModifiedTime());
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testCurrentDirectory(String dir) throws IOException {
            SFTPPath path = createPath(dir);
            // times are in seconds
            FileTime lastModifiedTime = FileTime.from(123456789L, TimeUnit.SECONDS);
            provider().getFileAttributeView(path, BasicFileAttributeView.class).setTimes(lastModifiedTime, null, null);
            assertEquals(lastModifiedTime, provider().readAttributes(path, BasicFileAttributes.class).lastModifiedTime());
        }

        @Test
        void testNonExisting() {
            SFTPPath path = createPath("/foo/bar");
            BasicFileAttributeView fileAttributeView = provider().getFileAttributeView(path, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
            FileTime lastModifiedTime = FileTime.from(123456789L, TimeUnit.SECONDS);

            FileSystemException exception = assertThrows(FileSystemException.class, () -> fileAttributeView.setTimes(lastModifiedTime, null, null));
            assertEquals("/foo/bar", exception.getFile());

            verify(getExceptionFactory()).createSetModificationTimeException(eq("/foo/bar"), any(SftpException.class));
        }

        // cannot test followLinks true/false on Windows because symbolic links have the same time as their targets
    }

    @Nested
    class SetAttribute {

        @Test
        void testLastModifiedTime() throws IOException {
            Path foo = addDirectory("/foo");

            SFTPPath fooPath = createPath("/foo");
            FileTime lastModifiedTime = FileTime.from(123456L, TimeUnit.SECONDS);

            provider().setAttribute(fooPath, "basic:lastModifiedTime", lastModifiedTime);
            assertEquals(lastModifiedTime, Files.getLastModifiedTime(foo));

            lastModifiedTime = FileTime.from(1234567L, TimeUnit.SECONDS);

            provider().setAttribute(fooPath, "posix:lastModifiedTime", lastModifiedTime);
            assertEquals(lastModifiedTime, Files.getLastModifiedTime(foo));

            lastModifiedTime = FileTime.from(12345678L, TimeUnit.SECONDS);

            provider().setAttribute(fooPath, "lastModifiedTime", lastModifiedTime);
            assertEquals(lastModifiedTime, Files.getLastModifiedTime(foo));
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = { "owner:owner", "posix:owner" })
        void testOwner(String attribute) throws IOException {
            addDirectory("/foo");

            SFTPPath fooPath = createPath("/foo");

            provider().setAttribute(fooPath, attribute, new SimpleUserPrincipal("1"));

            Map<String, Object> expected = new HashMap<>();
            expected.put("uid", 1);
            expected.put("gid", Integer.valueOf(provider().readAttributes(fooPath, PosixFileAttributes.class).group().getName()));

            assertAttributesModified(fooPath, expected);
        }

        @Test
        void testPermissions() throws IOException {
            addDirectory("/foo");

            SFTPPath fooPath = createPath("/foo");
            Set<PosixFilePermission> permissions = EnumSet.of(PosixFilePermission.OWNER_READ);

            provider().setAttribute(fooPath, "posix:permissions", permissions);

            assertAttributesModified(fooPath, Collections.singletonMap("permissions", permissions));
        }

        @Test
        void testGroup() throws IOException {
            addDirectory("/foo");

            SFTPPath fooPath = createPath("/foo");

            provider().setAttribute(fooPath, "posix:group", new SimpleGroupPrincipal("1"));

            Map<String, Object> expected = new HashMap<>();
            expected.put("uid", Integer.valueOf(provider().getFileAttributeView(fooPath, FileOwnerAttributeView.class).getOwner().getName()));
            expected.put("gid", 1);

            assertAttributesModified(fooPath, expected);
        }

        @ParameterizedTest
        @ValueSource(strings = CURRENT_DIR)
        @EmptySource
        void testCurrentDirectory(String dir) throws IOException {
            Path foo = getPath(getDefaultDir());

            SFTPPath fooPath = createPath(dir);
            FileTime lastModifiedTime = FileTime.from(123456L, TimeUnit.SECONDS);

            provider().setAttribute(fooPath, "basic:lastModifiedTime", lastModifiedTime);
            assertEquals(lastModifiedTime, Files.getLastModifiedTime(foo));

            lastModifiedTime = FileTime.from(1234567L, TimeUnit.SECONDS);

            provider().setAttribute(fooPath, "posix:lastModifiedTime", lastModifiedTime);
            assertEquals(lastModifiedTime, Files.getLastModifiedTime(foo));

            lastModifiedTime = FileTime.from(12345678L, TimeUnit.SECONDS);

            provider().setAttribute(fooPath, "lastModifiedTime", lastModifiedTime);
            assertEquals(lastModifiedTime, Files.getLastModifiedTime(foo));
        }

        @ParameterizedTest(name = "{0}")
        @CsvSource({
                "lastAccessTime, lastAccessTime",
                "creationTime, creationTime",
                "basic:lastAccessTime, lastAccessTime",
                "basic:creationTime, creationTime",
                "posix:lastAccessTime, lastAccessTime",
                "posix:creationTime, creationTime"
        })
        void testUnsupportedAttribute(String attributeToSet, String reportedAttribute) throws IOException {
            addDirectory("/foo");

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                    () -> provider.setAttribute(path, attributeToSet, true));
            assertChainEquals(Messages.fileSystemProvider().unsupportedFileAttribute(reportedAttribute), exception);
        }

        @ParameterizedTest(name = "{0}")
        @CsvSource({
                "basic:owner, owner",
                "basic:permissions, permissions",
                "basic:group, group",
                "owner:permissions, permissions",
                "owner:group, group",
                "owner:lastModifiedTime, lastModifiedTime"
        })
        void testSupportedAttributeForWrongView(String attributeToSet, String reportedAttribute) throws IOException {
            addDirectory("/foo");

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                    () -> provider.setAttribute(path, attributeToSet, true));
            assertChainEquals(Messages.fileSystemProvider().unsupportedFileAttribute(reportedAttribute), exception);
        }

        @Test
        void testUnsupportedType() throws IOException {
            addDirectory("/foo");

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                    () -> provider.setAttribute(path, "zipfs:size", true));
            assertChainEquals(Messages.fileSystemProvider().unsupportedFileAttributeView("zipfs"), exception);
        }

        @Test
        void testInvalidValueType() throws IOException {
            addDirectory("/foo");

            SFTPFileSystemProvider provider = provider();
            SFTPPath path = createPath("/foo");

            assertThrows(ClassCastException.class, () -> provider.setAttribute(path, "lastModifiedTime", 1));
        }
    }

    @Test
    void testGetTotalSpace() throws IOException {
        // SshServer does not support statVFS
        assertEquals(Long.MAX_VALUE, createPath("/").getTotalSpace());
    }

    @Test
    void testGetUsableSpace() throws IOException {
        // SshServer does not support statVFS
        assertEquals(Long.MAX_VALUE, createPath("/").getUsableSpace());
    }

    @Test
    void testGetUnallocatedSpace() throws IOException {
        // SshServer does not support statVFS
        assertEquals(Long.MAX_VALUE, createPath("/").getUnallocatedSpace());
    }
}
