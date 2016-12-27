/*
 * SFTPPath.java
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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import com.github.robtimus.filesystems.Messages;
import com.github.robtimus.filesystems.SimpleAbstractPath;

/**
 * A path for SFTP file systems.
 *
 * @author Rob Spoor
 */
class SFTPPath extends SimpleAbstractPath {

    private final SFTPFileSystem fs;

    SFTPPath(SFTPFileSystem fs, String path) {
        super(path);
        this.fs = Objects.requireNonNull(fs);
    }

    private SFTPPath(SFTPFileSystem fs, String path, boolean normalized) {
        super(path, normalized);
        this.fs = Objects.requireNonNull(fs);
    }

    @Override
    protected SFTPPath createPath(String path) {
        return new SFTPPath(fs, path, true);
    }

    @Override
    public FileSystem getFileSystem() {
        return fs;
    }

    @Override
    public SFTPPath getRoot() {
        return (SFTPPath) super.getRoot();
    }

    @Override
    public SFTPPath getFileName() {
        return (SFTPPath) super.getFileName();
    }

    @Override
    public SFTPPath getParent() {
        return (SFTPPath) super.getParent();
    }

    @Override
    public SFTPPath getName(int index) {
        return (SFTPPath) super.getName(index);
    }

    @Override
    public SFTPPath subpath(int beginIndex, int endIndex) {
        return (SFTPPath) super.subpath(beginIndex, endIndex);
    }

    @Override
    public SFTPPath normalize() {
        return (SFTPPath) super.normalize();
    }

    @Override
    public SFTPPath resolve(Path other) {
        return (SFTPPath) super.resolve(other);
    }

    @Override
    public SFTPPath resolve(String other) {
        return (SFTPPath) super.resolve(other);
    }

    @Override
    public SFTPPath resolveSibling(Path other) {
        return (SFTPPath) super.resolveSibling(other);
    }

    @Override
    public SFTPPath resolveSibling(String other) {
        return (SFTPPath) super.resolveSibling(other);
    }

    @Override
    public SFTPPath relativize(Path other) {
        return (SFTPPath) super.relativize(other);
    }

    @Override
    public URI toUri() {
        return fs.toUri(this);
    }

    @Override
    public SFTPPath toAbsolutePath() {
        return fs.toAbsolutePath(this);
    }

    @Override
    public SFTPPath toRealPath(LinkOption... options) throws IOException {
        return fs.toRealPath(this, options);
    }

    @Override
    public WatchKey register(WatchService watcher, Kind<?>[] events, Modifier... modifiers) throws IOException {
        throw Messages.unsupportedOperation(Path.class, "register"); //$NON-NLS-1$
    }

    @Override
    public String toString() {
        return fs.toString(this);
    }

    InputStream newInputStream(OpenOption... options) throws IOException {
        return fs.newInputStream(this, options);
    }

    OutputStream newOutputStream(OpenOption... options) throws IOException {
        return fs.newOutputStream(this, options);
    }

    SeekableByteChannel newByteChannel(Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        return fs.newByteChannel(this, options, attrs);
    }

    DirectoryStream<Path> newDirectoryStream(Filter<? super Path> filter) throws IOException {
        return fs.newDirectoryStream(this, filter);
    }

    void createDirectory(FileAttribute<?>... attrs) throws IOException {
        fs.createDirectory(this, attrs);
    }

    void delete() throws IOException {
        fs.delete(this);
    }

    SFTPPath readSymbolicLink() throws IOException {
        return fs.readSymbolicLink(this);
    }

    void copy(SFTPPath target, CopyOption... options) throws IOException {
        fs.copy(this, target, options);
    }

    void move(SFTPPath target, CopyOption... options) throws IOException {
        fs.move(this, target, options);
    }

    boolean isSameFile(Path other) throws IOException {
        if (this.equals(other)) {
            return true;
        }
        if (other == null || getFileSystem() != other.getFileSystem()) {
            return false;
        }
        return fs.isSameFile(this, (SFTPPath) other);
    }

    boolean isHidden() throws IOException {
        return fs.isHidden(this);
    }

    FileStore getFileStore() throws IOException {
        return fs.getFileStore(this);
    }

    void checkAccess(AccessMode... modes) throws IOException {
        fs.checkAccess(this, modes);
    }

    PosixFileAttributes readAttributes(LinkOption... options) throws IOException {
        return fs.readAttributes(this, options);
    }

    Map<String, Object> readAttributes(String attributes, LinkOption... options) throws IOException {
        return fs.readAttributes(this, attributes, options);
    }

    void setOwner(UserPrincipal owner) throws IOException {
        fs.setOwner(this, owner);
    }

    void setGroup(GroupPrincipal group) throws IOException {
        fs.setGroup(this, group);
    }

    void setPermissions(Set<PosixFilePermission> permissions) throws IOException {
        fs.setPermissions(this, permissions);
    }

    void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        fs.setTimes(this, lastModifiedTime, lastAccessTime, createTime);
    }

    void setAttribute(String attribute, Object value, LinkOption... options) throws IOException {
        fs.setAttribute(this, attribute, value, options);
    }
}
