/*
 * SFTPFileStoreTest.java
 * Copyright 2023 Rob Spoor
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;

@SuppressWarnings("nls")
class SFTPFileStoreTest extends AbstractSFTPFileSystemTest {

    private SFTPFileStore fileStore;

    @BeforeEach
    void initFileStore() throws IOException {
        addFile("/foo/bar");

        SFTPPath path = createPath("/foo/bar");
        fileStore = (SFTPFileStore) provider().getFileStore(path);
    }

    @Test
    void testName() {
        assertEquals(getBaseUrl() + "/", fileStore.name());
    }

    @Test
    void testType() {
        assertEquals("sftp", fileStore.type());
    }

    @Test
    void testIsReadOnly() {
        assertFalse(fileStore.isReadOnly());
    }

    @Test
    void testGetTotalSpace() throws IOException {
        // SshServer does not support statVFS
        assertEquals(Long.MAX_VALUE, fileStore.getTotalSpace());
    }

    @Test
    void testGetUsableSpace() throws IOException {
        // SshServer does not support statVFS
        assertEquals(Long.MAX_VALUE, fileStore.getUsableSpace());
    }

    @Test
    void testGetUnallocatedSpace() throws IOException {
        // SshServer does not support statVFS
        assertEquals(Long.MAX_VALUE, fileStore.getUnallocatedSpace());
    }

    @Test
    void testGetBlockSize() {
        // SshServer does not support statVFS
        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class, fileStore::getBlockSize);
        SftpException cause = assertInstanceOf(SftpException.class, exception.getCause());
        assertEquals(ChannelSftp.SSH_FX_OP_UNSUPPORTED, cause.id);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(classes = { BasicFileAttributeView.class, FileOwnerAttributeView.class, PosixFileAttributeView.class })
    void testSupportsFileAttributeView(Class<? extends FileAttributeView> type) {
        assertTrue(fileStore.supportsFileAttributeView(type));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(classes = { DosFileAttributeView.class, AclFileAttributeView.class })
    void testSupportsFileAttributeViewNotSupported(Class<? extends FileAttributeView> type) {
        assertFalse(fileStore.supportsFileAttributeView(type));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = { "basic", "owner", "posix" })
    void testSupportsFileAttributeView(String name) {
        assertTrue(fileStore.supportsFileAttributeView(name));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = { "dos", "acl" })
    void testSupportsFileAttributeViewNotSupported(String name) {
        assertFalse(fileStore.supportsFileAttributeView(name));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = { "totalSpace", "usableSpace", "unallocatedSpace" })
    void testGetAttribute(String attribute) throws IOException {
        assertNotNull(fileStore.getAttribute(attribute));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = { "size", "owner" })
    void testGetAttributeNotSupported(String attribute) {
        assertThrows(UnsupportedOperationException.class, () -> fileStore.getAttribute(attribute));
    }
}
