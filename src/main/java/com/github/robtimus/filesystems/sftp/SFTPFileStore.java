/*
 * SFTPFileStore.java
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

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.util.Objects;
import com.github.robtimus.filesystems.Messages;

/**
 * An FTP file system store.
 *
 * @author Rob Spoor
 */
class SFTPFileStore extends FileStore {

    private final SFTPFileSystem fs;

    SFTPFileStore(SFTPFileSystem fs) {
        this.fs = Objects.requireNonNull(fs);
    }

    @Override
    public String name() {
        return fs.toUri("/").toString(); //$NON-NLS-1$
    }

    @Override
    public String type() {
        return "sftp"; //$NON-NLS-1$
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public long getTotalSpace() throws IOException {
        return fs.getTotalSpace();
    }

    @Override
    public long getUsableSpace() throws IOException {
        return fs.getUsableSpace();
    }

    @Override
    public long getUnallocatedSpace() throws IOException {
        return fs.getUnallocatedSpace();
    }

    @Override
    public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
        return type == BasicFileAttributeView.class || type == PosixFileAttributeView.class;
    }

    @Override
    @SuppressWarnings("nls")
    public boolean supportsFileAttributeView(String name) {
        return "basic".equals(name) || "owner".equals(name) || "posix".equals(name);
    }

    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
        Objects.requireNonNull(type);
        return null;
    }

    @Override
    public Object getAttribute(String attribute) throws IOException {
        if ("totalSpace".equals(attribute)) { //$NON-NLS-1$
            return getTotalSpace();
        }
        if ("usableSpace".equals(attribute)) { //$NON-NLS-1$
            return getUsableSpace();
        }
        if ("unallocatedSpace".equals(attribute)) { //$NON-NLS-1$
            return getUnallocatedSpace();
        }
        throw Messages.fileStore().unsupportedAttribute(attribute);
    }
}
