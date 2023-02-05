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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import com.github.robtimus.filesystems.Messages;
import com.github.robtimus.filesystems.attribute.SimpleGroupPrincipal;
import com.github.robtimus.filesystems.attribute.SimpleUserPrincipal;
import com.jcraft.jsch.SftpException;

@SuppressWarnings("nls")
class SFTPFileSystemTest extends AbstractSFTPFileSystemTest {

    // SFTPFileSystem.getPath

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

    // SFTPFileSystem.keepAlive

    @Test
    void testKeepAlive() {
        assertDoesNotThrow(fileSystem::keepAlive);
    }

    // SFTPFileSystem.toUri

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

    // SFTPFileSystem.toAbsolutePath

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

    // SFTPFileSystem.toRealPath

    @Test
    void testToRealPathNoFollowLinks() throws IOException {
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

        testToRealPathNoFollowLinks("/", "/");
        testToRealPathNoFollowLinks("/foo/bar", "/foo/bar");
        testToRealPathNoFollowLinks("/foo/../bar", "/bar");

        testToRealPathNoFollowLinks("", "/home");
        testToRealPathNoFollowLinks("foo/bar", "/home/foo/bar");
        testToRealPathNoFollowLinks("foo/../bar", "/home/bar");

        // symbolic links
        testToRealPathNoFollowLinks("/hello", "/hello");
        testToRealPathNoFollowLinks("/world", "/world");
        testToRealPathNoFollowLinks("/home/baz", "/home/baz");
        testToRealPathNoFollowLinks("/baz", "/baz");
    }

    private void testToRealPathNoFollowLinks(String path, String expected) throws IOException {
        SFTPPath expectedPath = createPath(expected);
        Path actual = createPath(path).toRealPath(LinkOption.NOFOLLOW_LINKS);
        assertEquals(expectedPath, actual);
    }

    @Test
    void testToRealPathFollowLinks() throws IOException {
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

        testToRealPathFollowLinks("/", "/");
        testToRealPathFollowLinks("/foo/bar", "/foo/bar");
        testToRealPathFollowLinks("/foo/../bar", "/bar");

        testToRealPathFollowLinks("", "/home");
        testToRealPathFollowLinks("foo/bar", "/home/foo/bar");
        testToRealPathFollowLinks("foo/../bar", "/home/bar");

        // symbolic links
        testToRealPathFollowLinks("/hello", "/foo");
        testToRealPathFollowLinks("/world", "/foo");
        testToRealPathFollowLinks("/home/baz", "/home/bar");
        testToRealPathFollowLinks("/baz", "/home/bar");
    }

    private void testToRealPathFollowLinks(String path, String expected) throws IOException {
        SFTPPath expectedPath = createPath(expected);
        Path actual = createPath(path).toRealPath();
        assertEquals(expectedPath, actual);
    }

    @Test
    void testToRealPathBrokenLink() throws IOException {
        addSymLink("/foo", getPath("/bar"));

        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> createPath("/foo").toRealPath());
        assertEquals("/bar", exception.getFile());
    }

    @Test
    void testToRealPathNotExisting() {
        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> createPath("/foo").toRealPath());
        assertEquals("/foo", exception.getFile());
    }

    // SFTPFileSystem.newInputStream

    @Test
    void testNewInputStream() throws IOException {
        addFile("/foo/bar");

        try (InputStream input = provider().newInputStream(createPath("/foo/bar"))) {
            // don't do anything with the stream, there's a separate test for that
        }
        // verify that the file system can be used after closing the stream
        provider().checkAccess(createPath("/foo/bar"));
    }

    @Test
    void testNewInputStreamDeleteOnClose() throws IOException {
        addFile("/foo/bar");

        OpenOption[] options = { StandardOpenOption.DELETE_ON_CLOSE };
        try (InputStream input = provider().newInputStream(createPath("/foo/bar"), options)) {
            // don't do anything with the stream, there's a separate test for that
        }
        assertFalse(Files.exists(getPath("/foo/bar")));
        assertEquals(0, getChildCount("/foo"));
    }

    @Test
    void testNewInputStreamSFTPFailure() {
        // failure: file not found

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo/bar");

        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.newInputStream(path));
        assertEquals("/foo/bar", exception.getFile());

        verify(getExceptionFactory()).createNewInputStreamException(eq("/foo/bar"), any(SftpException.class));
    }

    // SFTPFileSystem.newOutputStream

    @Test
    void testNewOutputStreamExisting() throws IOException {
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
    void testNewOutputStreamExistingDeleteOnClose() throws IOException {
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
    void testNewOutputStreamExistingCreate() throws IOException {
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
    void testNewOutputStreamExistingCreateDeleteOnClose() throws IOException {
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
    void testNewOutputStreamExistingCreateNew() throws IOException {
        addDirectory("/foo");
        addFile("/foo/bar");

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo/bar");
        OpenOption[] options = { StandardOpenOption.CREATE_NEW };

        FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.newOutputStream(path, options));
        assertEquals("/foo/bar", exception.getFile());

        // verify that the file system can be used after closing the stream
        assertDoesNotThrow(() -> provider().checkAccess(createPath("/foo/bar")));
        assertTrue(Files.isDirectory(getPath("/foo")));
        assertTrue(Files.isRegularFile(getPath("/foo/bar")));
    }

    @Test
    void testNewOutputStreamExistingSFTPFailure() throws IOException {
        addDirectory("/foo");
        Path bar = addFile("/foo/bar");
        bar.toFile().setReadOnly();

        // failure: no permission to write

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo/bar");
        OpenOption[] options = { StandardOpenOption.WRITE };

        FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.newOutputStream(path, options));
        assertEquals("/foo/bar", exception.getFile());

        verify(getExceptionFactory()).createNewOutputStreamException(eq("/foo/bar"), any(SftpException.class), anyCollection());
        assertTrue(Files.isDirectory(getPath("/foo")));
        assertTrue(Files.isRegularFile(getPath("/foo/bar")));
    }

    @Test
    void testNewOutputStreamExistingSFTPFailureDeleteOnClose() throws IOException {
        addDirectory("/foo");
        Path bar = addFile("/foo/bar");
        bar.toFile().setReadOnly();

        // failure: no permission to write

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo/bar");
        OpenOption[] options = { StandardOpenOption.DELETE_ON_CLOSE };

        FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.newOutputStream(path, options));
        assertEquals("/foo/bar", exception.getFile());

        verify(getExceptionFactory()).createNewOutputStreamException(eq("/foo/bar"), any(SftpException.class), anyCollection());
        assertTrue(Files.isDirectory(getPath("/foo")));
        assertTrue(Files.isRegularFile(getPath("/foo/bar")));
    }

    @Test
    void testNewOutputStreamNonExistingNoCreate() throws IOException {
        addDirectory("/foo");

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo/bar");
        OpenOption[] options = { StandardOpenOption.WRITE };

        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.newOutputStream(path, options));
        assertEquals("/foo/bar", exception.getFile());

        assertTrue(Files.isDirectory(getPath("/foo")));
        assertFalse(Files.exists(getPath("/foo/bar")));
    }

    @Test
    void testNewOutputStreamNonExistingCreate() throws IOException {
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
    void testNewOutputStreamNonExistingCreateDeleteOnClose() throws IOException {
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
    void testNewOutputStreamNonExistingCreateNew() throws IOException {
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
    void testNewOutputStreamNonExistingCreateNewDeleteOnClose() throws IOException {
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
    void testNewOutputStreamDirectoryNoCreate() throws IOException {
        addDirectory("/foo");

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");
        OpenOption[] options = { StandardOpenOption.WRITE };

        FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.newOutputStream(path, options));
        assertEquals("/foo", exception.getFile());
        assertEquals(Messages.fileSystemProvider().isDirectory("/foo").getReason(), exception.getReason());

        verify(getExceptionFactory(), never()).createNewOutputStreamException(anyString(), any(SftpException.class), anyCollection());
        assertTrue(Files.isDirectory(getPath("/foo")));
        assertEquals(0, getChildCount("/foo"));
    }

    @Test
    void testNewOutputStreamDirectoryDeleteOnClose() throws IOException {
        addDirectory("/foo");

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");
        OpenOption[] options = { StandardOpenOption.DELETE_ON_CLOSE };

        FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.newOutputStream(path, options));
        assertEquals("/foo", exception.getFile());
        assertEquals(Messages.fileSystemProvider().isDirectory("/foo").getReason(), exception.getReason());

        verify(getExceptionFactory(), never()).createNewOutputStreamException(anyString(), any(SftpException.class), anyCollection());
        assertTrue(Files.isDirectory(getPath("/foo")));
        assertEquals(0, getChildCount("/foo"));
    }

    // SFTPFileSystem.newByteChannel

    @Test
    void testNewByteChannelRead() throws IOException {
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
    void testNewByteChannelReadNonExisting() {

        // failure: file does not exist

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo/bar");
        Set<? extends OpenOption> options = EnumSet.noneOf(StandardOpenOption.class);

        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.newByteChannel(path, options));
        assertEquals("/foo/bar", exception.getFile());

        verify(getExceptionFactory()).createNewInputStreamException(eq("/foo/bar"), any(SftpException.class));
        assertFalse(Files.exists(getPath("/foo")));
        assertFalse(Files.exists(getPath("/foo/bar")));
    }

    @Test
    void testNewByteChannelWrite() throws IOException {
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
    void testNewByteChannelWriteAppend() throws IOException {
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
    void testNewByteChannelCreateWriteExisting() throws IOException {
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
    void testNewByteChannelCreateAppendExisting() throws IOException {
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
    void testNewByteChannelCreateWriteNonExisting() throws IOException {
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
    void testNewByteChannelCreateAppendNonExisting() throws IOException {
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
    void testNewByteChannelCreateNewWriteExisting() throws IOException {
        Path bar = addFile("/foo/bar");
        byte[] oldContents = Files.readAllBytes(bar);

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo/bar");
        Set<? extends OpenOption> options = EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

        FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.newByteChannel(path, options));
        assertEquals("/foo/bar", exception.getFile());

        assertArrayEquals(oldContents, Files.readAllBytes(bar));
    }

    @Test
    void testNewByteChannelCreateNewAppendExisting() throws IOException {
        Path bar = addFile("/foo/bar");
        setContents(bar, new byte[1024]);

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo/bar");
        Set<? extends OpenOption> options = EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND);

        FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.newByteChannel(path, options));
        assertEquals("/foo/bar", exception.getFile());

        assertArrayEquals(new byte[1024], Files.readAllBytes(bar));
    }

    @Test
    void testNewByteChannelCreateNewWriteNonExisting() throws IOException {
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
    void testNewByteChannelCreateNewAppendNonExisting() throws IOException {
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

    // SFTPFileSystem.newDirectoryStream

    @Test
    void testNewDirectoryStream() throws IOException {

        try (DirectoryStream<Path> stream = provider().newDirectoryStream(createPath("/"), entry -> true)) {
            assertNotNull(stream);
            // don't do anything with the stream, there's a separate test for that
        }
    }

    @Test
    void testNewDirectoryStreamNotExisting() {
        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");

        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.newDirectoryStream(path, entry -> true));
        assertEquals("/foo", exception.getFile());
    }

    @Test
    void testGetDirectoryStreamNotDirectory() throws IOException {
        addFile("/foo");

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");

        NotDirectoryException exception = assertThrows(NotDirectoryException.class, () -> provider.newDirectoryStream(path, entry -> true));
        assertEquals("/foo", exception.getFile());
    }

    // SFTPFileSystem.createNewDirectory

    @Test
    void testCreateDirectory() throws IOException {
        assertFalse(Files.exists(getPath("/foo")));

        provider().createDirectory(createPath("/foo"));

        assertTrue(Files.isDirectory(getPath("/foo")));
    }

    @Test
    void testCreateDirectoryAlreadyExists() throws IOException {
        addDirectory("/foo/bar");

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo/bar");

        FileAlreadyExistsException exception = assertThrows(FileAlreadyExistsException.class, () -> provider.createDirectory(path));
        assertEquals("/foo/bar", exception.getFile());

        verify(getExceptionFactory(), never()).createCreateDirectoryException(anyString(), any(SftpException.class));
        assertTrue(Files.exists(getPath("/foo")));
        assertTrue(Files.exists(getPath("/foo/bar")));
    }

    @Test
    void testCreateDirectorySFTPFailure() {
        // failure: parent does not exist

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo/bar");

        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.createDirectory(path));
        assertEquals("/foo/bar", exception.getFile());

        verify(getExceptionFactory()).createCreateDirectoryException(eq("/foo/bar"), any(SftpException.class));
        assertFalse(Files.exists(getPath("/foo")));
        assertFalse(Files.exists(getPath("/foo/bar")));
    }

    // SFTPFileSystem.delete

    @Test
    void testDeleteNonExisting() {
        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");

        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.delete(path));
        assertEquals("/foo", exception.getFile());

        verify(getExceptionFactory(), never()).createDeleteException(eq("/foo"), any(SftpException.class), anyBoolean());
    }

    @Test
    void testDeleteRoot() {
        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/");

        FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.delete(path));
        assertEquals("/", exception.getFile());

        verify(getExceptionFactory()).createDeleteException(eq("/"), any(SftpException.class), eq(true));
    }

    @Test
    void testDeleteFile() throws IOException {
        addFile("/foo/bar");

        provider().delete(createPath("/foo/bar"));

        assertTrue(Files.isDirectory(getPath("/foo")));
        assertFalse(Files.exists(getPath("/foo/bar")));
    }

    @Test
    void testDeleteEmptyDir() throws IOException {
        addDirectory("/foo/bar");

        provider().delete(createPath("/foo/bar"));

        assertTrue(Files.isDirectory(getPath("/foo")));
        assertFalse(Files.exists(getPath("/foo/bar")));
    }

    @Test
    void testDeleteSFTPFailure() throws IOException {
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

    // SFTPFileSystem.readSymbolicLink

    @Test
    void testReadSymbolicLinkToFile() throws IOException {
        Path foo = addFile("/foo");
        addSymLink("/bar", foo);

        Path link = provider().readSymbolicLink(createPath("/bar"));
        assertEquals(createPath("/foo"), link);
    }

    @Test
    void testReadSymbolicLinkToDirectory() throws IOException {
        Path foo = addDirectory("/foo");
        addSymLink("/bar", foo);

        Path link = provider().readSymbolicLink(createPath("/bar"));
        assertEquals(createPath("/foo"), link);
    }

    @Test
    void testReadSymbolicLinkToNonExistingTarget() throws IOException {
        addSymLink("/bar", getPath("/foo"));

        Path link = provider().readSymbolicLink(createPath("/bar"));
        assertEquals(createPath("/foo"), link);
    }

    @Test
    void testReadSymbolicLinkNotExisting() {
        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");

        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.readSymbolicLink(path));
        assertEquals("/foo", exception.getFile());

        verify(getExceptionFactory()).createReadLinkException(eq("/foo"), any(SftpException.class));
    }

    @Test
    void testReadSymbolicLinkNoLinkButFile() throws IOException {
        addFile("/foo");

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");

        FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.readSymbolicLink(path));
        assertEquals("/foo", exception.getFile());

        verify(getExceptionFactory()).createReadLinkException(eq("/foo"), any(SftpException.class));
    }

    @Test
    void testReadSymbolicLinkNoLinkButDirectory() throws IOException {
        addDirectory("/foo");

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");

        FileSystemException exception = assertThrows(FileSystemException.class, () -> provider.readSymbolicLink(path));
        assertEquals("/foo", exception.getFile());

        verify(getExceptionFactory()).createReadLinkException(eq("/foo"), any(SftpException.class));
    }

    // SFTPFileSystem.copy

    @Test
    void testCopySame() throws IOException {
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
    void testCopyNonExisting() throws IOException {
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
    void testCopySFTPFailure() throws IOException {
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
    void testCopyRoot() throws IOException {
        // copying a directory (including the root) will not copy its contents, so copying the root is allowed
        addDirectory("/foo");

        CopyOption[] options = {};
        provider().copy(createPath("/"), createPath("/foo/bar"), options);

        assertTrue(Files.isDirectory(getPath("/foo")));
        assertTrue(Files.isDirectory(getPath("/foo/bar")));
        assertEquals(0, getChildCount("/foo/bar"));
    }

    @Test
    void testCopyReplaceFile() throws IOException {
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
    void testCopyReplaceFileAllowed() throws IOException {
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
    void testCopyReplaceNonEmptyDir() throws IOException {
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
    void testCopyReplaceNonEmptyDirAllowed() throws IOException {
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
    void testCopyReplaceEmptyDir() throws IOException {
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
    void testCopyReplaceEmptyDirAllowed() throws IOException {
        addDirectory("/foo");
        addDirectory("/baz");

        CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
        provider().copy(createPath("/baz"), createPath("/foo"), options);

        assertTrue(Files.isDirectory(getPath("/foo")));
        assertTrue(Files.isDirectory(getPath("/foo")));
    }

    @Test
    void testCopyFile() throws IOException {
        addDirectory("/foo");
        addFile("/baz");

        CopyOption[] options = {};
        provider().copy(createPath("/baz"), createPath("/foo/bar"), options);

        assertTrue(Files.isDirectory(getPath("/foo")));
        assertTrue(Files.isRegularFile(getPath("/foo/bar")));
        assertTrue(Files.isRegularFile(getPath("/baz")));
    }

    @Test
    void testCopyFileMultipleConnections() throws IOException {
        addDirectory("/foo");
        addFile("/baz");

        CopyOption[] options = {};
        fileSystem2.copy(createPath(fileSystem2, "/baz"), createPath(fileSystem2, "/foo/bar"), options);

        assertTrue(Files.isDirectory(getPath("/foo")));
        assertTrue(Files.isRegularFile(getPath("/foo/bar")));
        assertTrue(Files.isRegularFile(getPath("/baz")));
    }

    @Test
    void testCopyEmptyDir() throws IOException {
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
    void testCopyNonEmptyDir() throws IOException {
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

    @Test
    void testCopyReplaceFileDifferentFileSystems() throws IOException {
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
    void testCopyReplaceFileAllowedDifferentFileSystems() throws IOException {
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
    void testCopyReplaceNonEmptyDirDifferentFileSystems() throws IOException {
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
    void testCopyReplaceNonEmptyDirAllowedDifferentFileSystems() throws IOException {
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
    void testCopyReplaceEmptyDirDifferentFileSystems() throws IOException {
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
    void testCopyReplaceEmptyDirAllowedDifferentFileSystems() throws IOException {
        addDirectory("/foo");
        addDirectory("/baz");

        CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
        provider().copy(createPath("/baz"), createPath(fileSystem2, "/foo"), options);

        assertTrue(Files.isDirectory(getPath("/foo")));
        assertTrue(Files.isDirectory(getPath("/foo")));
    }

    @Test
    void testCopyFileDifferentFileSystems() throws IOException {
        addDirectory("/foo");
        addFile("/baz");

        CopyOption[] options = {};
        provider().copy(createPath("/baz"), createPath(fileSystem2, "/foo/bar"), options);

        assertTrue(Files.isDirectory(getPath("/foo")));
        assertTrue(Files.isRegularFile(getPath("/foo/bar")));
        assertTrue(Files.isRegularFile(getPath("/baz")));
    }

    @Test
    void testCopyEmptyDirDifferentFileSystems() throws IOException {
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
    void testCopyNonEmptyDirDifferentFileSystems() throws IOException {
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
    void testCopyWithAttributes() throws IOException {
        addDirectory("/foo");
        addDirectory("/baz");
        addFile("/baz/qux");

        SFTPFileSystemProvider provider = provider();
        SFTPPath source = createPath("/baz");
        SFTPPath target = createPath("/foo/bar");
        CopyOption[] options = { StandardCopyOption.COPY_ATTRIBUTES };

        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class, () -> provider.copy(source, target, options));
        assertEquals(Messages.fileSystemProvider().unsupportedCopyOption(StandardCopyOption.COPY_ATTRIBUTES).getMessage(), exception.getMessage());
    }

    // SFTPFileSystem.move

    @Test
    void testMoveSame() throws IOException {
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
    void testMoveNonExisting() throws IOException {
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
    void testMoveSFTPFailure() throws IOException {
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
    void testMoveEmptyRoot() {

        SFTPFileSystemProvider provider = provider();
        SFTPPath source = createPath("/");
        SFTPPath target = createPath("/baz");
        CopyOption[] options = {};

        DirectoryNotEmptyException exception = assertThrows(DirectoryNotEmptyException.class, () -> provider.move(source, target, options));
        assertEquals("/", exception.getFile());

        assertFalse(Files.exists(getPath("/baz")));
    }

    @Test
    void testMoveNonEmptyRoot() throws IOException {
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
    void testMoveReplaceFile() throws IOException {
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
    void testMoveReplaceFileAllowed() throws IOException {
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
    void testMoveReplaceEmptyDir() throws IOException {
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
    void testMoveReplaceEmptyDirAllowed() throws IOException {
        addDirectory("/foo");
        addFile("/baz");

        CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
        provider().move(createPath("/baz"), createPath("/foo"), options);

        assertTrue(Files.isRegularFile(getPath("/foo")));
        assertFalse(Files.exists(getPath("/baz")));
    }

    @Test
    void testMoveFile() throws IOException {
        addDirectory("/foo");
        addFile("/baz");

        CopyOption[] options = {};
        provider().move(createPath("/baz"), createPath("/foo/bar"), options);

        assertTrue(Files.isDirectory(getPath("/foo")));
        assertTrue(Files.isRegularFile(getPath("/foo/bar")));
        assertFalse(Files.exists(getPath("/baz")));
    }

    @Test
    void testMoveEmptyDir() throws IOException {
        addDirectory("/foo");
        addDirectory("/baz");

        CopyOption[] options = {};
        provider().move(createPath("/baz"), createPath("/foo/bar"), options);

        assertTrue(Files.isDirectory(getPath("/foo")));
        assertTrue(Files.isDirectory(getPath("/foo/bar")));
        assertFalse(Files.exists(getPath("/baz")));
    }

    @Test
    void testMoveNonEmptyDir() throws IOException {
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
    void testMoveNonEmptyDirSameParent() throws IOException {
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

    @Test
    void testMoveReplaceFileDifferentFileSystem() throws IOException {
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
    void testMoveReplaceFileAllowedDifferentFileSystem() throws IOException {
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
    void testMoveReplaceEmptyDirDifferentFileSystem() throws IOException {
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

    void testMoveReplaceEmptyDirAllowedDifferentFileSystem() throws IOException {
        addDirectory("/foo");
        addFile("/baz");

        CopyOption[] options = { StandardCopyOption.REPLACE_EXISTING };
        provider().move(createPath("/baz"), createPath(fileSystem2, "/foo"), options);

        assertTrue(Files.isRegularFile(getPath("/foo")));
        assertFalse(Files.exists(getPath("/baz")));
    }

    @Test
    void testMoveFileDifferentFileSystem() throws IOException {
        addDirectory("/foo");
        addFile("/baz");

        CopyOption[] options = {};
        provider().move(createPath("/baz"), createPath(fileSystem2, "/foo/bar"), options);

        assertTrue(Files.isDirectory(getPath("/foo")));
        assertTrue(Files.isRegularFile(getPath("/foo/bar")));
        assertFalse(Files.exists(getPath("/baz")));
    }

    @Test
    void testMoveEmptyDirDifferentFileSystem() throws IOException {
        addDirectory("/foo");
        addDirectory("/baz");

        CopyOption[] options = {};
        provider().move(createPath("/baz"), createPath(fileSystem2, "/foo/bar"), options);

        assertTrue(Files.isDirectory(getPath("/foo")));
        assertTrue(Files.isDirectory(getPath("/foo/bar")));
        assertFalse(Files.exists(getPath("/baz")));
    }

    @Test
    void testMoveNonEmptyDirDifferentFileSystem() throws IOException {
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

    // SFTPFileSystem.isSameFile

    @Test
    void testIsSameFileEquals() throws IOException {

        assertTrue(provider().isSameFile(createPath("/"), createPath("/")));
        assertTrue(provider().isSameFile(createPath("/foo"), createPath("/foo")));
        assertTrue(provider().isSameFile(createPath("/foo/bar"), createPath("/foo/bar")));

        assertTrue(provider().isSameFile(createPath(""), createPath("")));
        assertTrue(provider().isSameFile(createPath("foo"), createPath("foo")));
        assertTrue(provider().isSameFile(createPath("foo/bar"), createPath("foo/bar")));

        assertTrue(provider().isSameFile(createPath(""), createPath("/home")));
        assertTrue(provider().isSameFile(createPath("/home"), createPath("")));
    }

    @Test
    void testIsSameFileExisting() throws IOException {
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
    void testIsSameFileFirstNonExisting() {
        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");
        SFTPPath path2 = createPath("/");

        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.isSameFile(path, path2));
        assertEquals("/foo", exception.getFile());
    }

    @Test
    void testIsSameFileSecondNonExisting() {
        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/");
        SFTPPath path2 = createPath("/foo");

        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.isSameFile(path, path2));
        assertEquals("/foo", exception.getFile());
    }

    // SFTPFileSystem.isHidden

    @Test
    void testIsHidden() throws IOException {
        addDirectory("/foo");
        addDirectory("/.foo");
        addFile("/foo/bar");
        addFile("/foo/.bar");

        assertFalse(provider().isHidden(createPath("/foo")));
        assertTrue(provider().isHidden(createPath("/.foo")));
        assertFalse(provider().isHidden(createPath("/foo/bar")));
        assertTrue(provider().isHidden(createPath("/foo/.bar")));
    }

    @Test
    void testIsHiddenNonExisting() {
        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");

        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.isHidden(path));
        assertEquals("/foo", exception.getFile());
    }

    // SFTPFileSystem.checkAccess

    @Test
    void testCheckAccessNonExisting() {
        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo/bar");

        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.checkAccess(path));
        assertEquals("/foo/bar", exception.getFile());
    }

    @Test
    void testCheckAccessNoModes() throws IOException {
        addDirectory("/foo/bar");

        provider().checkAccess(createPath("/foo/bar"));
    }

    @Test
    void testCheckAccessOnlyRead() throws IOException {
        addDirectory("/foo/bar");

        provider().checkAccess(createPath("/foo/bar"), AccessMode.READ);
    }

    @Test
    void testCheckAccessOnlyWriteNotReadOnly() throws IOException {
        addDirectory("/foo/bar");

        provider().checkAccess(createPath("/foo/bar"), AccessMode.WRITE);
    }

    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "On Windows, the permissions are not reported correctly, but always as rw-rw-rw-")
    void testCheckAccessOnlyWriteReadOnly() throws IOException {
        Path bar = addDirectory("/foo/bar");
        bar.toFile().setReadOnly();

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo/bar");

        AccessDeniedException exception = assertThrows(AccessDeniedException.class, () -> provider.checkAccess(path, AccessMode.WRITE));
        assertEquals("/foo/bar", exception.getFile());
    }

    @Test
    void testCheckAccessOnlyExecute() throws IOException {
        Path bar = addFile("/foo/bar");
        bar.toFile().setReadOnly();

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo/bar");

        AccessDeniedException exception = assertThrows(AccessDeniedException.class, () -> provider.checkAccess(path, AccessMode.EXECUTE));
        assertEquals("/foo/bar", exception.getFile());
    }

    // SFTPFileSystem.readAttributes (SFTPFileAttributes variant)

    @Test
    void testReadAttributesFileFollowLinks() throws IOException {
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
    void testReadAttributesFileNoFollowLinks() throws IOException {
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
    void testReadAttributesDirectoryFollowLinksForDirectory() throws IOException {
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
    void testReadAttributesDirectoryFollowLinksForFile() throws IOException {
        Path foo = addFile("/foo");

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
    void testReadAttributesDirectoryNoFollowLinksForDirectory() throws IOException {
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

    @Test
    void testReadAttributesDirectoryNoFollowLinksForFile() throws IOException {
        Path foo = addFile("/foo");

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
    void testReadAttributesSymLinkToFileFollowLinks() throws IOException {
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
    void testReadAttributesSymLinkToFileNoFollowLinks() throws IOException {
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
    void testReadAttributesSymLinkToDirectoryFollowLinks() throws IOException {
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
    void testReadAttributesSymLinkToDirectoryNoFollowLinks() throws IOException {
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
    void testReadAttributesNonExisting() {
        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");

        NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> provider.readAttributes(path, PosixFileAttributes.class));
        assertEquals("/foo", exception.getFile());
    }

    // SFTPFileSystem.readAttributes (map variant)

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
            "lastModifiedTime, basic:lastModifiedTime",
            "basic:lastModifiedTime, basic:lastModifiedTime",
            "posix:lastModifiedTime, posix:lastModifiedTime"
    })
    void testReadAttributesMapLastModifiedTime(String attributeName, String expectedKey) throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
        assertEquals(Collections.singleton(expectedKey), attributes.keySet());
        assertNotNull(attributes.get(expectedKey));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
            "lastAccessTime, basic:lastAccessTime",
            "basic:lastAccessTime, basic:lastAccessTime",
            "posix:lastAccessTime, posix:lastAccessTime"
    })
    void testReadAttributesMapLastAccessTime(String attributeName, String expectedKey) throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
        assertEquals(Collections.singleton(expectedKey), attributes.keySet());
        assertNotNull(attributes.get(expectedKey));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
            "creationTime, basic:creationTime",
            "basic:creationTime, basic:creationTime",
            "posix:creationTime, posix:creationTime"
    })
    void testReadAttributesMapCreateTime(String attributeName, String expectedKey) throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
        assertEquals(Collections.singleton(expectedKey), attributes.keySet());
        assertNotNull(attributes.get(expectedKey));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
            "size, basic:size",
            "basic:size, basic:size",
            "posix:size, posix:size"
    })
    void testReadAttributesMapSize(String attributeName, String expectedKey) throws IOException {
        Path foo = addFile("/foo");
        setContents(foo, new byte[1024]);

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
        Map<String, ?> expected = Collections.singletonMap(expectedKey, Files.size(foo));
        assertEquals(expected, attributes);
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
            "isRegularFile, basic:isRegularFile",
            "basic:isRegularFile, basic:isRegularFile",
            "posix:isRegularFile, posix:isRegularFile"
    })
    void testReadAttributesMapIsRegularFile(String attributeName, String expectedKey) throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
        Map<String, ?> expected = Collections.singletonMap(expectedKey, false);
        assertEquals(expected, attributes);
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
            "isDirectory, basic:isDirectory",
            "basic:isDirectory, basic:isDirectory",
            "posix:isDirectory, posix:isDirectory"
    })
    void testReadAttributesMapIsDirectory(String attributeName, String expectedKey) throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
        Map<String, ?> expected = Collections.singletonMap(expectedKey, true);
        assertEquals(expected, attributes);
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
            "isSymbolicLink, basic:isSymbolicLink",
            "basic:isSymbolicLink, basic:isSymbolicLink",
            "posix:isSymbolicLink, posix:isSymbolicLink"
    })
    void testReadAttributesMapIsSymbolicLink(String attributeName, String expectedKey) throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
        Map<String, ?> expected = Collections.singletonMap(expectedKey, false);
        assertEquals(expected, attributes);
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
            "isOther, basic:isOther",
            "basic:isOther, basic:isOther",
            "posix:isOther, posix:isOther"
    })
    void testReadAttributesMapIsOther(String attributeName, String expectedKey) throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
        Map<String, ?> expected = Collections.singletonMap(expectedKey, false);
        assertEquals(expected, attributes);
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
            "fileKey, basic:fileKey",
            "basic:fileKey, basic:fileKey",
            "posix:fileKey, posix:fileKey"
    })
    void testReadAttributesMapFileKey(String attributeName, String expectedKey) throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), attributeName);
        Map<String, ?> expected = Collections.singletonMap(expectedKey, null);
        assertEquals(expected, attributes);
    }

    @Test
    void testReadAttributesMapNoTypeMultipleForDirectory() throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "size,isDirectory");
        Map<String, Object> expected = new HashMap<>();
        // Directories always have size 0 when using sshd-core
        expected.put("basic:size", 0L);
        expected.put("basic:isDirectory", true);
        assertEquals(expected, attributes);
    }

    @Test
    void testReadAttributesMapNoTypeMultipleForFile() throws IOException {
        Path foo = addFile("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "size,isRegularFile");
        Map<String, Object> expected = new HashMap<>();
        expected.put("basic:size", Files.size(foo));
        expected.put("basic:isRegularFile", true);
        assertEquals(expected, attributes);
    }

    @Test
    void testReadAttributesMapNoTypeAllForDirectory() throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "*");
        Map<String, Object> expected = new HashMap<>();
        // Directories always have size 0 when using sshd-core
        expected.put("basic:size", 0L);
        expected.put("basic:isRegularFile", false);
        expected.put("basic:isDirectory", true);
        expected.put("basic:isSymbolicLink", false);
        expected.put("basic:isOther", false);
        expected.put("basic:fileKey", null);

        assertNotNull(attributes.remove("basic:lastModifiedTime"));
        assertNotNull(attributes.remove("basic:lastAccessTime"));
        assertNotNull(attributes.remove("basic:creationTime"));
        assertEquals(expected, attributes);

        attributes = provider().readAttributes(createPath("/foo"), "basic:lastModifiedTime,*");
        assertNotNull(attributes.remove("basic:lastModifiedTime"));
        assertNotNull(attributes.remove("basic:lastAccessTime"));
        assertNotNull(attributes.remove("basic:creationTime"));
        assertEquals(expected, attributes);
    }

    @Test
    void testReadAttributesMapNoTypeAllForFile() throws IOException {
        Path foo = addFile("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "*");
        Map<String, Object> expected = new HashMap<>();
        expected.put("basic:size", Files.size(foo));
        expected.put("basic:isRegularFile", true);
        expected.put("basic:isDirectory", false);
        expected.put("basic:isSymbolicLink", false);
        expected.put("basic:isOther", false);
        expected.put("basic:fileKey", null);

        assertNotNull(attributes.remove("basic:lastModifiedTime"));
        assertNotNull(attributes.remove("basic:lastAccessTime"));
        assertNotNull(attributes.remove("basic:creationTime"));
        assertEquals(expected, attributes);

        attributes = provider().readAttributes(createPath("/foo"), "basic:lastModifiedTime,*");
        assertNotNull(attributes.remove("basic:lastModifiedTime"));
        assertNotNull(attributes.remove("basic:lastAccessTime"));
        assertNotNull(attributes.remove("basic:creationTime"));
        assertEquals(expected, attributes);
    }

    @Test
    void testReadAttributesMapBasicMultipleForDirectory() throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "basic:size,isDirectory");
        Map<String, Object> expected = new HashMap<>();
        // Directories always have size 0 when using sshd-core
        expected.put("basic:size", 0L);
        expected.put("basic:isDirectory", true);
        assertEquals(expected, attributes);
    }

    @Test
    void testReadAttributesMapBasicMultipleForFile() throws IOException {
        Path foo = addFile("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "basic:size,isRegularFile");
        Map<String, Object> expected = new HashMap<>();
        expected.put("basic:size", Files.size(foo));
        expected.put("basic:isRegularFile", true);
        assertEquals(expected, attributes);
    }

    @Test
    void testReadAttributesMapBasicAllForDirectory() throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "basic:*");
        Map<String, Object> expected = new HashMap<>();
        // Directories always have size 0 when using sshd-core
        expected.put("basic:size", 0L);
        expected.put("basic:isRegularFile", false);
        expected.put("basic:isDirectory", true);
        expected.put("basic:isSymbolicLink", false);
        expected.put("basic:isOther", false);
        expected.put("basic:fileKey", null);

        assertNotNull(attributes.remove("basic:lastModifiedTime"));
        assertNotNull(attributes.remove("basic:lastAccessTime"));
        assertNotNull(attributes.remove("basic:creationTime"));
        assertEquals(expected, attributes);

        attributes = provider().readAttributes(createPath("/foo"), "basic:lastModifiedTime,*");
        assertNotNull(attributes.remove("basic:lastModifiedTime"));
        assertNotNull(attributes.remove("basic:lastAccessTime"));
        assertNotNull(attributes.remove("basic:creationTime"));
        assertEquals(expected, attributes);
    }

    @Test
    void testReadAttributesMapBasicAllForFile() throws IOException {
        Path foo = addFile("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "basic:*");
        Map<String, Object> expected = new HashMap<>();
        expected.put("basic:size", Files.size(foo));
        expected.put("basic:isRegularFile", true);
        expected.put("basic:isDirectory", false);
        expected.put("basic:isSymbolicLink", false);
        expected.put("basic:isOther", false);
        expected.put("basic:fileKey", null);

        assertNotNull(attributes.remove("basic:lastModifiedTime"));
        assertNotNull(attributes.remove("basic:lastAccessTime"));
        assertNotNull(attributes.remove("basic:creationTime"));
        assertEquals(expected, attributes);

        attributes = provider().readAttributes(createPath("/foo"), "basic:lastModifiedTime,*");
        assertNotNull(attributes.remove("basic:lastModifiedTime"));
        assertNotNull(attributes.remove("basic:lastAccessTime"));
        assertNotNull(attributes.remove("basic:creationTime"));
        assertEquals(expected, attributes);
    }

    @Test
    void testReadAttributesMapOwnerOwner() throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "owner:owner");
        assertEquals(Collections.singleton("owner:owner"), attributes.keySet());
        assertNotNull(attributes.get("owner:owner"));
    }

    @Test
    void testReadAttributesMapOwnerAll() throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "owner:*");
        assertEquals(Collections.singleton("owner:owner"), attributes.keySet());
        assertNotNull(attributes.get("owner:owner"));

        attributes = provider().readAttributes(createPath("/foo"), "owner:owner,*");
        assertEquals(Collections.singleton("owner:owner"), attributes.keySet());
        assertNotNull(attributes.get("owner:owner"));
    }

    @Test
    void testReadAttributesMapPosixOwner() throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "posix:owner");
        assertEquals(Collections.singleton("posix:owner"), attributes.keySet());
        assertNotNull(attributes.get("posix:owner"));
    }

    @Test
    void testReadAttributesMapPosixGroup() throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "posix:group");
        assertEquals(Collections.singleton("posix:group"), attributes.keySet());
        assertNotNull(attributes.get("posix:group"));
    }

    @Test
    void testReadAttributesMapPosixPermissions() throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "posix:permissions");
        assertEquals(Collections.singleton("posix:permissions"), attributes.keySet());
        assertNotNull(attributes.get("posix:permissions"));
    }

    @Test
    void testReadAttributesMapPosixMultipleForDirectory() throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "posix:size,owner,group");
        // Directories always have size 0 when using sshd-core
        Map<String, ?> expected = Collections.singletonMap("posix:size", 0L);
        assertNotNull(attributes.remove("posix:owner"));
        assertNotNull(attributes.remove("posix:group"));
        assertEquals(expected, attributes);
    }

    @Test
    void testReadAttributesMapPosixMultipleForFile() throws IOException {
        Path foo = addFile("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "posix:size,owner,group");
        Map<String, ?> expected = Collections.singletonMap("posix:size", Files.size(foo));
        assertNotNull(attributes.remove("posix:owner"));
        assertNotNull(attributes.remove("posix:group"));
        assertEquals(expected, attributes);
    }

    @Test
    void testReadAttributesMapPosixAllForDirectory() throws IOException {
        addDirectory("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "posix:*");
        Map<String, Object> expected = new HashMap<>();
        // Directories always have size 0 when using sshd-core
        expected.put("posix:size", 0L);
        expected.put("posix:isRegularFile", false);
        expected.put("posix:isDirectory", true);
        expected.put("posix:isSymbolicLink", false);
        expected.put("posix:isOther", false);
        expected.put("posix:fileKey", null);

        assertNotNull(attributes.remove("posix:lastModifiedTime"));
        assertNotNull(attributes.remove("posix:lastAccessTime"));
        assertNotNull(attributes.remove("posix:creationTime"));
        assertNotNull(attributes.remove("posix:owner"));
        assertNotNull(attributes.remove("posix:group"));
        assertNotNull(attributes.remove("posix:permissions"));
        assertEquals(expected, attributes);

        attributes = provider().readAttributes(createPath("/foo"), "posix:lastModifiedTime,*");
        assertNotNull(attributes.remove("posix:lastModifiedTime"));
        assertNotNull(attributes.remove("posix:lastAccessTime"));
        assertNotNull(attributes.remove("posix:creationTime"));
        assertNotNull(attributes.remove("posix:owner"));
        assertNotNull(attributes.remove("posix:group"));
        assertNotNull(attributes.remove("posix:permissions"));
        assertEquals(expected, attributes);
    }

    @Test
    void testReadAttributesMapPosixAllForFile() throws IOException {
        Path foo = addFile("/foo");

        Map<String, Object> attributes = provider().readAttributes(createPath("/foo"), "posix:*");
        Map<String, Object> expected = new HashMap<>();
        expected.put("posix:size", Files.size(foo));
        expected.put("posix:isRegularFile", true);
        expected.put("posix:isDirectory", false);
        expected.put("posix:isSymbolicLink", false);
        expected.put("posix:isOther", false);
        expected.put("posix:fileKey", null);

        assertNotNull(attributes.remove("posix:lastModifiedTime"));
        assertNotNull(attributes.remove("posix:lastAccessTime"));
        assertNotNull(attributes.remove("posix:creationTime"));
        assertNotNull(attributes.remove("posix:owner"));
        assertNotNull(attributes.remove("posix:group"));
        assertNotNull(attributes.remove("posix:permissions"));
        assertEquals(expected, attributes);

        attributes = provider().readAttributes(createPath("/foo"), "posix:lastModifiedTime,*");
        assertNotNull(attributes.remove("posix:lastModifiedTime"));
        assertNotNull(attributes.remove("posix:lastAccessTime"));
        assertNotNull(attributes.remove("posix:creationTime"));
        assertNotNull(attributes.remove("posix:owner"));
        assertNotNull(attributes.remove("posix:group"));
        assertNotNull(attributes.remove("posix:permissions"));
        assertEquals(expected, attributes);
    }

    @Test
    void testReadAttributesMapUnsupportedAttribute() throws IOException {
        addDirectory("/foo");

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> provider.readAttributes(path, "posix:lastModifiedTime,owner,dummy"));
        assertEquals(Messages.fileSystemProvider().unsupportedFileAttribute("dummy").getMessage(), exception.getMessage());
    }

    @Test
    void testReadAttributesMapUnsupportedType() throws IOException {
        addDirectory("/foo");

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");

        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class, () -> provider.readAttributes(path, "zipfs:*"));
        assertEquals(Messages.fileSystemProvider().unsupportedFileAttributeView("zipfs").getMessage(), exception.getMessage());
    }

    // SFTPFileSystem.setOwner

    // the success flow cannot be properly tested on Windows

    @Test
    void testSetOwnerNonExisting() {
        SFTPPath path = createPath("/foo/bar");
        PosixFileAttributeView fileAttributeView = provider().getFileAttributeView(path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        UserPrincipal owner = new SimpleUserPrincipal("1");

        FileSystemException exception = assertThrows(FileSystemException.class, () -> fileAttributeView.setOwner(owner));
        assertEquals("/foo/bar", exception.getFile());

        verify(getExceptionFactory()).createSetOwnerException(eq("/foo/bar"), any(SftpException.class));
    }

    @Test
    void testSetOwnerNonNumeric() throws IOException {
        addFile("/foo/bar");

        PosixFileAttributeView fileAttributeView = provider().getFileAttributeView(createPath("/foo/bar"), PosixFileAttributeView.class);
        UserPrincipal owner = new SimpleUserPrincipal("test");

        IOException exception = assertThrows(IOException.class, () -> fileAttributeView.setOwner(owner));
        assertThat(exception.getCause(), instanceOf(NumberFormatException.class));

        verify(getExceptionFactory(), never()).createSetOwnerException(anyString(), any(SftpException.class));
    }

    // SFTPFileSystem.setGroup

    // the success flow cannot be properly tested on Windows

    @Test
    void testSetGroupNonExisting() {
        SFTPPath path = createPath("/foo/bar");
        PosixFileAttributeView fileAttributeView = provider().getFileAttributeView(path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        GroupPrincipal group = new SimpleGroupPrincipal("1");

        FileSystemException exception = assertThrows(FileSystemException.class, () -> fileAttributeView.setGroup(group));
        assertEquals("/foo/bar", exception.getFile());

        verify(getExceptionFactory()).createSetGroupException(eq("/foo/bar"), any(SftpException.class));
    }

    @Test
    void testSetGroupNonNumeric() throws IOException {
        addFile("/foo/bar");

        PosixFileAttributeView fileAttributeView = provider().getFileAttributeView(createPath("/foo/bar"), PosixFileAttributeView.class);
        GroupPrincipal group = new SimpleGroupPrincipal("test");

        IOException exception = assertThrows(IOException.class, () -> fileAttributeView.setGroup(group));
        assertThat(exception.getCause(), instanceOf(NumberFormatException.class));

        verify(getExceptionFactory(), never()).createSetOwnerException(anyString(), any(SftpException.class));
    }

    // SFTPFileSystem.setPermissions

    // the success flow cannot be properly tested on Windows

    @Test
    void testSetPermissionsNonExisting() {
        SFTPPath path = createPath("/foo/bar");
        PosixFileAttributeView fileAttributeView = provider().getFileAttributeView(path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("r--r--r--");

        FileSystemException exception = assertThrows(FileSystemException.class, () -> fileAttributeView.setPermissions(permissions));
        assertEquals("/foo/bar", exception.getFile());

        verify(getExceptionFactory()).createSetPermissionsException(eq("/foo/bar"), any(SftpException.class));
    }

    // SFTPFileSystem.setLastModifiedTime

    @Test
    void testSetLastModifiedTimeExisting() throws IOException {
        addFile("/foo/bar");

        SFTPPath path = createPath("/foo/bar");
        // times are in seconds
        FileTime lastModifiedTime = FileTime.from(123456789L, TimeUnit.SECONDS);
        provider().getFileAttributeView(path, BasicFileAttributeView.class).setTimes(lastModifiedTime, null, null);
        assertEquals(lastModifiedTime, provider().readAttributes(path, BasicFileAttributes.class).lastModifiedTime());
    }

    @Test
    void testSetLastModifiedTimeNonExisting() {
        SFTPPath path = createPath("/foo/bar");
        BasicFileAttributeView fileAttributeView = provider().getFileAttributeView(path, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        FileTime lastModifiedTime = FileTime.from(123456789L, TimeUnit.SECONDS);

        FileSystemException exception = assertThrows(FileSystemException.class, () -> fileAttributeView.setTimes(lastModifiedTime, null, null));
        assertEquals("/foo/bar", exception.getFile());

        verify(getExceptionFactory()).createSetModificationTimeException(eq("/foo/bar"), any(SftpException.class));
    }

    // cannot test followLinks true/false on Windows because symbolic links have the same time as their targets

    // SFTPFileSystem.setAttribute

    @Test
    void testSetAttributeLastModifiedTime() throws IOException {
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

    @Test
    void testSetAttributeUnsupportedAttribute() throws IOException {
        addDirectory("/foo");

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> provider.setAttribute(path, "basic:creationTime", true));
        assertEquals(Messages.fileSystemProvider().unsupportedFileAttribute("basic:creationTime").getMessage(), exception.getMessage());
    }

    @Test
    void testSetAttributeUnsupportedType() throws IOException {
        addDirectory("/foo");

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");

        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> provider.setAttribute(path, "zipfs:size", true));
        assertEquals(Messages.fileSystemProvider().unsupportedFileAttributeView("zipfs").getMessage(), exception.getMessage());
    }

    @Test
    void testSetAttributeInvalidValueType() throws IOException {
        addDirectory("/foo");

        SFTPFileSystemProvider provider = provider();
        SFTPPath path = createPath("/foo");

        assertThrows(ClassCastException.class, () -> provider.setAttribute(path, "basic:lastModifiedTime", 1));
    }

    // SFTPFileSystem.getTotalSpace

    @Test
    void testGetTotalSpace() throws IOException {
        // SshServer does not support statVFS
        assertEquals(Long.MAX_VALUE, createPath("/").getTotalSpace());
    }

    // SFTPFileSystem.getUsableSpace

    @Test
    void testGetUsableSpace() throws IOException {
        // SshServer does not support statVFS
        assertEquals(Long.MAX_VALUE, createPath("/").getUsableSpace());
    }

    // SFTPFileSystem.getUnallocatedSpace

    @Test
    void testGetUnallocatedSpace() throws IOException {
        // SshServer does not support statVFS
        assertEquals(Long.MAX_VALUE, createPath("/").getUnallocatedSpace());
    }
}
