/*
 * SFTPFileSystem.java
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
import static com.github.robtimus.filesystems.SimpleAbstractPath.PARENT_DIR;
import static com.github.robtimus.filesystems.SimpleAbstractPath.ROOT_PATH;
import static com.github.robtimus.filesystems.SimpleAbstractPath.SEPARATOR;
import static com.github.robtimus.filesystems.attribute.FileAttributeConstants.BASIC_VIEW;
import static com.github.robtimus.filesystems.attribute.FileAttributeConstants.CREATION_TIME;
import static com.github.robtimus.filesystems.attribute.FileAttributeConstants.FILE_OWNER_VIEW;
import static com.github.robtimus.filesystems.attribute.FileAttributeConstants.LAST_ACCESS_TIME;
import static com.github.robtimus.filesystems.attribute.FileAttributeConstants.POSIX_VIEW;
import static com.github.robtimus.filesystems.attribute.FileAttributeSupport.getAttributeName;
import static com.github.robtimus.filesystems.attribute.FileAttributeSupport.getAttributeNames;
import static com.github.robtimus.filesystems.attribute.FileAttributeSupport.getViewName;
import static com.github.robtimus.filesystems.attribute.FileAttributeSupport.populateAttributeMap;
import static com.github.robtimus.filesystems.attribute.FileAttributeViewMetadata.BASIC;
import static com.github.robtimus.filesystems.attribute.FileAttributeViewMetadata.FILE_OWNER;
import static com.github.robtimus.filesystems.attribute.FileAttributeViewMetadata.POSIX;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import com.github.robtimus.filesystems.AbstractDirectoryStream;
import com.github.robtimus.filesystems.FileSystemProviderSupport;
import com.github.robtimus.filesystems.Messages;
import com.github.robtimus.filesystems.PathMatcherSupport;
import com.github.robtimus.filesystems.URISupport;
import com.github.robtimus.filesystems.attribute.FileAttributeSupport;
import com.github.robtimus.filesystems.attribute.FileAttributeViewCollection;
import com.github.robtimus.filesystems.attribute.FileAttributeViewMetadata;
import com.github.robtimus.filesystems.attribute.PosixFilePermissionSupport;
import com.github.robtimus.filesystems.attribute.SimpleGroupPrincipal;
import com.github.robtimus.filesystems.attribute.SimpleUserPrincipal;
import com.github.robtimus.filesystems.sftp.SSHChannelPool.Channel;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpStatVFS;

/**
 * An SFTP file system.
 *
 * @author Rob Spoor
 */
class SFTPFileSystem extends FileSystem {

    static final FileAttributeViewCollection VIEWS = FileAttributeViewCollection.withViews(BASIC, FILE_OWNER, POSIX);

    private final SFTPFileSystemProvider provider;
    private final Iterable<Path> rootDirectories;
    private final Iterable<FileStore> fileStores;

    private final SSHChannelPool channelPool;
    private final URI uri;
    private final String defaultDirectory;

    private final AtomicBoolean open = new AtomicBoolean(true);

    SFTPFileSystem(SFTPFileSystemProvider provider, URI uri, SFTPEnvironment env) throws IOException {
        this.provider = Objects.requireNonNull(provider);

        SFTPPath rootPath = new SFTPPath(this, ROOT_PATH);
        this.rootDirectories = Set.of(rootPath);
        this.fileStores = Set.of(new SFTPFileStore(rootPath));

        this.channelPool = new SSHChannelPool(uri.getHost(), uri.getPort(), env);
        this.uri = Objects.requireNonNull(uri);

        try (Channel channel = channelPool.get()) {
            this.defaultDirectory = channel.pwd();
        }
    }

    @Override
    public SFTPFileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() throws IOException {
        if (open.getAndSet(false)) {
            provider.removeFileSystem(uri);
            channelPool.close();
        }
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String getSeparator() {
        return SEPARATOR;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return rootDirectories;
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        // TODO: get the actual file stores, instead of only returning the root file store
        return fileStores;
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return VIEWS.viewNames();
    }

    @Override
    public Path getPath(String first, String... more) {
        StringBuilder sb = new StringBuilder(first);
        for (String s : more) {
            sb.append(SEPARATOR).append(s);
        }
        return new SFTPPath(this, sb.toString());
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        final Pattern pattern = PathMatcherSupport.toPattern(syntaxAndPattern);
        return path -> pattern.matcher(path.toString()).matches();
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw Messages.unsupportedOperation(FileSystem.class, "getUserPrincipalLookupService"); //$NON-NLS-1$
    }

    @Override
    public WatchService newWatchService() throws IOException {
        throw Messages.unsupportedOperation(FileSystem.class, "newWatchService"); //$NON-NLS-1$
    }

    void keepAlive() throws IOException {
        channelPool.keepAlive();
    }

    URI toUri(SFTPPath path) {
        return toUri(toAbsolutePath(path).normalizedPath());
    }

    URI toUri(String path) {
        return URISupport.create(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), path, null, null);
    }

    SFTPPath toAbsolutePath(SFTPPath path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new SFTPPath(this, defaultDirectory + SEPARATOR + path.path());
    }

    SFTPPath toRealPath(SFTPPath path, boolean followLinks) throws IOException {
        try (Channel channel = channelPool.get()) {
            return toRealPath(channel, path, followLinks).path;
        }
    }

    private SFTPPathAndAttributesPair toRealPath(Channel channel, SFTPPath path, boolean followLinks) throws IOException {
        SFTPPath absPath = toAbsolutePath(path).normalize();
        SftpATTRS attributes = getAttributes(channel, absPath.path(), false);
        if (followLinks && attributes.isLink()) {
            SFTPPath link = readSymbolicLink(channel, absPath);
            return toRealPath(channel, link, followLinks);
        }
        return new SFTPPathAndAttributesPair(absPath, attributes);
    }

    private static final class SFTPPathAndAttributesPair {
        private final SFTPPath path;
        private final SftpATTRS attributes;

        private SFTPPathAndAttributesPair(SFTPPath path, SftpATTRS attributes) {
            this.path = path;
            this.attributes = attributes;
        }
    }

    private String normalizePath(SFTPPath path) {
        return path.toAbsolutePath().normalizedPath();
    }

    String toString(SFTPPath path) {
        return path.path();
    }

    InputStream newInputStream(SFTPPath path, OpenOption... options) throws IOException {
        OpenOptions openOptions = OpenOptions.forNewInputStream(options);

        try (Channel channel = channelPool.get()) {
            return newInputStream(channel, normalizePath(path), openOptions);
        }
    }

    private InputStream newInputStream(Channel channel, String path, OpenOptions options) throws IOException {
        assert options.read;

        return channel.newInputStream(path, options);
    }

    OutputStream newOutputStream(SFTPPath path, OpenOption... options) throws IOException {
        OpenOptions openOptions = OpenOptions.forNewOutputStream(options);

        try (Channel channel = channelPool.get()) {
            return newOutputStream(channel, normalizePath(path), false, openOptions).out;
        }
    }

    @SuppressWarnings("resource")
    private SFTPAttributesAndOutputStreamPair newOutputStream(Channel channel, String path, boolean requireAttributes, OpenOptions options)
            throws IOException {

        // retrieve the attributes unless create is true and createNew is false, because then the file can be created
        SftpATTRS attributes = null;
        if (!options.create || options.createNew) {
            attributes = findAttributes(channel, path, false);
            if (attributes != null && attributes.isDir()) {
                throw Messages.fileSystemProvider().isDirectory(path);
            }
            if (!options.createNew && attributes == null) {
                throw new NoSuchFileException(path);
            } else if (options.createNew && attributes != null) {
                throw new FileAlreadyExistsException(path);
            }
        }
        // else the file can be created if necessary

        if (attributes == null && requireAttributes) {
            attributes = findAttributes(channel, path, false);
        }

        OutputStream out = channel.newOutputStream(path, options);
        return new SFTPAttributesAndOutputStreamPair(attributes, out);
    }

    private static final class SFTPAttributesAndOutputStreamPair {

        private final SftpATTRS attributes;
        private final OutputStream out;

        private SFTPAttributesAndOutputStreamPair(SftpATTRS attributes, OutputStream out) {
            this.attributes = attributes;
            this.out = out;
        }
    }

    @SuppressWarnings("resource")
    SeekableByteChannel newByteChannel(SFTPPath path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        if (attrs.length > 0) {
            throw Messages.fileSystemProvider().unsupportedCreateFileAttribute(attrs[0].name());
        }

        OpenOptions openOptions = OpenOptions.forNewByteChannel(options);

        try (Channel channel = channelPool.get()) {
            String normalizedPath = normalizePath(path);
            if (openOptions.read) {
                // use findAttributes instead of getAttributes, to let the opening of the stream provide the correct error message
                SftpATTRS attributes = findAttributes(channel, normalizedPath, false);
                InputStream in = newInputStream(channel, normalizedPath, openOptions);
                long size = attributes == null ? 0 : attributes.getSize();
                return FileSystemProviderSupport.createSeekableByteChannel(in, size);
            }

            // if append then we need the attributes, to find the initial position of the channel
            boolean requireAttributes = openOptions.append;
            SFTPAttributesAndOutputStreamPair outPair = newOutputStream(channel, normalizedPath, requireAttributes, openOptions);
            long initialPosition = outPair.attributes == null ? 0 : outPair.attributes.getSize();
            if (openOptions.write && !openOptions.append) {
                initialPosition = 0;
            }
            return FileSystemProviderSupport.createSeekableByteChannel(outPair.out, initialPosition);
        }
    }

    DirectoryStream<Path> newDirectoryStream(SFTPPath path, Filter<? super Path> filter) throws IOException {
        try (Channel channel = channelPool.get()) {
            String normalizedPath = normalizePath(path);
            List<LsEntry> entries = channel.listFiles(normalizedPath);
            boolean isDirectory = false;
            for (Iterator<LsEntry> i = entries.iterator(); i.hasNext(); ) {
                LsEntry entry = i.next();
                String filename = entry.getFilename();
                if (CURRENT_DIR.equals(filename)) {
                    isDirectory = true;
                    i.remove();
                } else if (PARENT_DIR.equals(filename)) {
                    i.remove();
                }
            }

            if (!isDirectory) {
                // https://github.com/robtimus/sftp-fs/issues/4: don't fail immediately but check the attributes
                // Follow links to ensure the directory attribute can be read correctly
                SftpATTRS attributes = channel.readAttributes(normalizedPath, true);
                if (!attributes.isDir()) {
                    throw new NotDirectoryException(path.path());
                }
            }
            return new SFTPPathDirectoryStream(path, entries, filter);
        }
    }

    private static final class SFTPPathDirectoryStream extends AbstractDirectoryStream<Path> {

        private final SFTPPath path;
        private final List<LsEntry> entries;
        private Iterator<LsEntry> iterator;

        private SFTPPathDirectoryStream(SFTPPath path, List<LsEntry> entries, Filter<? super Path> filter) {
            super(filter);
            this.path = path;
            this.entries = entries;
        }

        @Override
        protected void setupIteration() {
            iterator = entries.iterator();
        }

        @Override
        protected Path getNext() throws IOException {
            return iterator.hasNext() ? path.resolve(iterator.next().getFilename()) : null;
        }
    }

    void createDirectory(SFTPPath path, FileAttribute<?>... attrs) throws IOException {
        if (attrs.length > 0) {
            throw Messages.fileSystemProvider().unsupportedCreateFileAttribute(attrs[0].name());
        }

        try (Channel channel = channelPool.get()) {
            channel.mkdir(normalizePath(path));
        }
    }

    void delete(SFTPPath path) throws IOException {
        try (Channel channel = channelPool.get()) {
            String normalizedPath = normalizePath(path);
            SftpATTRS attributes = getAttributes(channel, normalizedPath, false);
            boolean isDirectory = attributes.isDir();
            channel.delete(normalizedPath, isDirectory);
        }
    }

    SFTPPath readSymbolicLink(SFTPPath path) throws IOException {
        try (Channel channel = channelPool.get()) {
            SFTPPath absPath = toAbsolutePath(path).normalize();
            return readSymbolicLink(channel, absPath);
        }
    }

    private SFTPPath readSymbolicLink(Channel channel, SFTPPath path) throws IOException {
        String link = channel.readSymbolicLink(path.path());
        return path.resolveSibling(link);
    }

    void copy(SFTPPath source, SFTPPath target, CopyOption... options) throws IOException {
        boolean sameFileSystem = haveSameFileSystem(source, target);
        CopyOptions copyOptions = CopyOptions.forCopy(options);

        try (Channel channel = channelPool.get()) {
            // get the attributes to determine whether a directory needs to be created or a file needs to be copied
            // Files.copy specifies that for links, the final target must be copied
            SFTPPathAndAttributesPair sourcePair = toRealPath(channel, source, true);

            if (!sameFileSystem) {
                copyAcrossFileSystems(channel, source, sourcePair.attributes, target, copyOptions);
                return;
            }

            try {
                if (sourcePair.path.path().equals(toRealPath(channel, target, true).path.path())) {
                    // non-op, don't do a thing as specified by Files.copy
                    return;
                }
            } catch (@SuppressWarnings("unused") NoSuchFileException e) {
                // the target does not exist or either path is an invalid link, ignore the error and continue
            }

            String normalizedTarget = normalizePath(target);

            SftpATTRS targetAttributes = findAttributes(channel, normalizedTarget, false);

            if (targetAttributes != null) {
                if (copyOptions.replaceExisting) {
                    channel.delete(normalizedTarget, targetAttributes.isDir());
                } else {
                    throw new FileAlreadyExistsException(target.path());
                }
            }

            if (sourcePair.attributes.isDir()) {
                channel.mkdir(normalizedTarget);
            } else {
                try (Channel channel2 = channelPool.getOrCreate()) {
                    copyFile(channel, normalizePath(source), channel2, normalizedTarget, copyOptions);
                }
            }
        }
    }

    private void copyAcrossFileSystems(Channel sourceChannel, SFTPPath source, SftpATTRS sourceAttributes, SFTPPath target, CopyOptions options)
            throws IOException {

        copyAcrossFileSystems(sourceChannel, normalizePath(source), sourceAttributes, target, options);
    }

    @SuppressWarnings("resource")
    private void copyAcrossFileSystems(Channel sourceChannel, String source, SftpATTRS sourceAttributes, SFTPPath target, CopyOptions options)
            throws IOException {

        copyAcrossFileSystems(sourceChannel, source, sourceAttributes, normalizePath(target), target.getFileSystem(), options);
    }

    private void copyAcrossFileSystems(Channel sourceChannel, String source, SftpATTRS sourceAttributes, String target,
            SFTPFileSystem targetFileSystem, CopyOptions options) throws IOException {

        try (Channel targetChannel = targetFileSystem.channelPool.getOrCreate()) {

            SftpATTRS targetAttributes = findAttributes(targetChannel, target, false);

            if (targetAttributes != null) {
                if (options.replaceExisting) {
                    targetChannel.delete(target, targetAttributes.isDir());
                } else {
                    throw new FileAlreadyExistsException(target);
                }
            }

            if (sourceAttributes.isDir()) {
                targetChannel.mkdir(target);
            } else {
                copyFile(sourceChannel, source, targetChannel, target, options);
            }
        }
    }

    private void copyFile(Channel sourceChannel, String source, Channel targetChannel, String target, CopyOptions options) throws IOException {
        OpenOptions inOptions = OpenOptions.forNewInputStream(options.toOpenOptions(StandardOpenOption.READ));
        OpenOptions outOptions = OpenOptions
                .forNewOutputStream(options.toOpenOptions(StandardOpenOption.WRITE, StandardOpenOption.CREATE));
        try (InputStream in = sourceChannel.newInputStream(source, inOptions)) {
            targetChannel.storeFile(target, in, outOptions.options);
        }
    }

    void move(SFTPPath source, SFTPPath target, CopyOption... options) throws IOException {
        boolean sameFileSystem = haveSameFileSystem(source, target);
        CopyOptions copyOptions = CopyOptions.forMove(sameFileSystem, options);

        try (Channel channel = channelPool.get()) {
            String normalizedSource = normalizePath(source);
            if (!sameFileSystem) {
                SftpATTRS attributes = getAttributes(channel, normalizedSource, false);
                if (attributes.isLink()) {
                    throw new IOException(SFTPMessages.copyOfSymbolicLinksAcrossFileSystemsNotSupported());
                }
                copyAcrossFileSystems(channel, normalizedSource, attributes, target, copyOptions);
                channel.delete(normalizedSource, attributes.isDir());
                return;
            }

            try {
                if (isSameFile(channel, source, target)) {
                    // non-op, don't do a thing as specified by Files.move
                    return;
                }
            } catch (@SuppressWarnings("unused") NoSuchFileException e) {
                // the source or target does not exist or either path is an invalid link
                // call getAttributes to ensure the source file exists
                // ignore any error to target or if the source link is invalid
                getAttributes(channel, normalizedSource, false);
            }

            if (ROOT_PATH.equals(normalizedSource)) {
                // cannot move or rename the root
                throw new DirectoryNotEmptyException(source.path());
            }

            String normalizedTarget = normalizePath(target);
            SftpATTRS targetAttributes = findAttributes(channel, normalizedTarget, false);
            if (copyOptions.replaceExisting && targetAttributes != null) {
                channel.delete(normalizedTarget, targetAttributes.isDir());
            }

            channel.rename(normalizedSource, normalizedTarget);
        }
    }

    boolean isSameFile(SFTPPath path, SFTPPath path2) throws IOException {
        if (!haveSameFileSystem(path, path2)) {
            return false;
        }
        if (path.equals(path2)) {
            return true;
        }
        try (Channel channel = channelPool.get()) {
            return isSameFile(channel, path, path2);
        }
    }

    @SuppressWarnings("resource")
    private boolean haveSameFileSystem(SFTPPath path, SFTPPath path2) {
        return path.getFileSystem() == path2.getFileSystem();
    }

    private boolean isSameFile(Channel channel, SFTPPath path, SFTPPath path2) throws IOException {
        if (path.equals(path2)) {
            return true;
        }
        return toRealPath(channel, path, true).path.path().equals(toRealPath(channel, path2, true).path.path());
    }

    boolean isHidden(SFTPPath path) throws IOException {
        // call getAttributes to check for existence
        try (Channel channel = channelPool.get()) {
            getAttributes(channel, normalizePath(path), false);
        }
        String fileName = path.fileName();
        return !CURRENT_DIR.equals(fileName) && !PARENT_DIR.equals(fileName) && fileName.startsWith("."); //$NON-NLS-1$
    }

    FileStore getFileStore(SFTPPath path) throws IOException {
        // call getAttributes to check for existence
        try (Channel channel = channelPool.get()) {
            getAttributes(channel, normalizePath(path), false);
        }
        return new SFTPFileStore(path);
    }

    void checkAccess(SFTPPath path, AccessMode... modes) throws IOException {
        try (Channel channel = channelPool.get()) {
            SftpATTRS attributes = getAttributes(channel, normalizePath(path), true);
            for (AccessMode mode : modes) {
                if (!hasAccess(attributes, mode)) {
                    throw new AccessDeniedException(path.path());
                }
            }
        }
    }

    private boolean hasAccess(SftpATTRS attrs, AccessMode mode) {
        switch (mode) {
            case READ:
                return PosixFilePermissionSupport.hasPermission(attrs.getPermissions(), PosixFilePermission.OWNER_READ);
            case WRITE:
                return PosixFilePermissionSupport.hasPermission(attrs.getPermissions(), PosixFilePermission.OWNER_WRITE);
            case EXECUTE:
                return PosixFilePermissionSupport.hasPermission(attrs.getPermissions(), PosixFilePermission.OWNER_EXECUTE);
            default:
                return false;
        }
    }

    <V extends FileAttributeView> V getFileAttributeView(SFTPPath path, Class<V> type, boolean followLinks) {
        if (type == BasicFileAttributeView.class) {
            return type.cast(new SFTPPathAttributeView(BASIC_VIEW, path, followLinks));
        }
        if (type == FileOwnerAttributeView.class) {
            return type.cast(new SFTPPathAttributeView(FILE_OWNER_VIEW, path, followLinks));
        }
        if (type == PosixFileAttributeView.class) {
            return type.cast(new SFTPPathAttributeView(POSIX_VIEW, path, followLinks));
        }
        return null;
    }

    private final class SFTPPathAttributeView implements PosixFileAttributeView {

        private final String name;
        private final SFTPPath path;
        private final boolean followLinks;

        private SFTPPathAttributeView(String name, SFTPPath path, boolean followLinks) {
            this.name = Objects.requireNonNull(name);
            this.path = Objects.requireNonNull(path);
            this.followLinks = followLinks;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public UserPrincipal getOwner() throws IOException {
            return readAttributes().owner();
        }

        @Override
        public PosixFileAttributes readAttributes() throws IOException {
            return SFTPFileSystem.this.readAttributes(path, followLinks);
        }

        @Override
        public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
            if (lastAccessTime != null) {
                throw new IOException(Messages.fileSystemProvider().unsupportedFileAttribute("lastAccessTime")); //$NON-NLS-1$
            }
            if (createTime != null) {
                throw new IOException(Messages.fileSystemProvider().unsupportedFileAttribute("creationTime")); //$NON-NLS-1$
            }
            if (lastModifiedTime != null) {
                try (Channel channel = channelPool.get()) {
                    // times are in seconds
                    channel.setMtime(pathToUpdate(channel), lastModifiedTime.to(TimeUnit.SECONDS));
                }
            }
        }

        @Override
        public void setOwner(UserPrincipal owner) throws IOException {
            try {
                int uid = Integer.parseInt(owner.getName());
                try (Channel channel = channelPool.get()) {
                    channel.chown(pathToUpdate(channel), uid);
                }
            } catch (NumberFormatException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void setGroup(GroupPrincipal group) throws IOException {
            try {
                int gid = Integer.parseInt(group.getName());
                try (Channel channel = channelPool.get()) {
                    channel.chgrp(pathToUpdate(channel), gid);
                }
            } catch (NumberFormatException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void setPermissions(Set<PosixFilePermission> permissions) throws IOException {
            try (Channel channel = channelPool.get()) {
                channel.chmod(pathToUpdate(channel), PosixFilePermissionSupport.toMask(permissions));
            }
        }

        private String pathToUpdate(Channel channel) throws IOException {
            return followLinks ? toRealPath(channel, path, followLinks).path.path() : normalizePath(path);
        }
    }

    PosixFileAttributes readAttributes(SFTPPath path, boolean followLinks) throws IOException {

        try (Channel channel = channelPool.get()) {
            SftpATTRS attributes = getAttributes(channel, normalizePath(path), followLinks);
            return new SFTPPathFileAttributes(attributes);
        }
    }

    private static final class SFTPPathFileAttributes implements PosixFileAttributes {

        private final SftpATTRS attributes;

        private SFTPPathFileAttributes(SftpATTRS attributes) {
            this.attributes = attributes;
        }

        @Override
        public UserPrincipal owner() {
            String user = Integer.toString(attributes.getUId());
            return new SimpleUserPrincipal(user);
        }

        @Override
        public GroupPrincipal group() {
            String group = Integer.toString(attributes.getGId());
            return new SimpleGroupPrincipal(group);
        }

        @Override
        public Set<PosixFilePermission> permissions() {
            return PosixFilePermissionSupport.fromMask(attributes.getPermissions());
        }

        @Override
        public FileTime lastModifiedTime() {
            // times are in seconds
            return FileTime.from(attributes.getMTime(), TimeUnit.SECONDS);
        }

        @Override
        public FileTime lastAccessTime() {
            // times are in seconds
            return FileTime.from(attributes.getATime(), TimeUnit.SECONDS);
        }

        @Override
        public FileTime creationTime() {
            return lastModifiedTime();
        }

        @Override
        public boolean isRegularFile() {
            return attributes.isReg();
        }

        @Override
        public boolean isDirectory() {
            return attributes.isDir();
        }

        @Override
        public boolean isSymbolicLink() {
            return attributes.isLink();
        }

        @Override
        public boolean isOther() {
            return !(isRegularFile() || isDirectory() || isSymbolicLink());
        }

        @Override
        public long size() {
            return attributes.getSize();
        }

        @Override
        public Object fileKey() {
            return null;
        }
    }

    Map<String, Object> readAttributes(SFTPPath path, String attributes, boolean followLinks) throws IOException {
        String viewName = getViewName(attributes);
        FileAttributeViewMetadata view = VIEWS.getView(viewName);
        Set<String> attributeNames = getAttributeNames(attributes, view);

        PosixFileAttributes fileAttributes = readAttributes(path, followLinks);

        Map<String, Object> result = new HashMap<>();
        populateAttributeMap(result, fileAttributes, attributeNames);
        return result;
    }

    void setAttribute(SFTPPath path, String attribute, Object value, boolean followLinks) throws IOException {
        String viewName = getViewName(attribute);
        String attributeName = getAttributeName(attribute);

        // Special cases that are supported by FileAttributeSupport but not this class
        if (LAST_ACCESS_TIME.equals(attributeName) || CREATION_TIME.equals(attributeName)) {
            throw Messages.fileSystemProvider().unsupportedFileAttribute(attributeName);
        }

        switch (viewName) {
            case BASIC_VIEW:
                setBasicAttribute(path, attributeName, value, followLinks);
                break;
            case FILE_OWNER_VIEW:
                setFileOwnerAttribute(path, attributeName, value, followLinks);
                break;
            case POSIX_VIEW:
                setPosixAttribute(path, attributeName, value, followLinks);
                break;
            default:
                throw Messages.fileSystemProvider().unsupportedFileAttributeView(viewName);
        }
    }

    private void setBasicAttribute(SFTPPath path, String attribute, Object value, boolean followLinks) throws IOException {
        BasicFileAttributeView view = getFileAttributeView(path, BasicFileAttributeView.class, followLinks);
        FileAttributeSupport.setAttribute(attribute, value, view);
    }

    private void setFileOwnerAttribute(SFTPPath path, String attribute, Object value, boolean followLinks) throws IOException {
        FileOwnerAttributeView view = getFileAttributeView(path, FileOwnerAttributeView.class, followLinks);
        FileAttributeSupport.setAttribute(attribute, value, view);
    }

    private void setPosixAttribute(SFTPPath path, String attribute, Object value, boolean followLinks) throws IOException {
        PosixFileAttributeView view = getFileAttributeView(path, PosixFileAttributeView.class, followLinks);
        FileAttributeSupport.setAttribute(attribute, value, view);
    }

    private SftpATTRS getAttributes(Channel channel, String path, boolean followLinks) throws IOException {
        return channel.readAttributes(path, followLinks);
    }

    private SftpATTRS findAttributes(Channel channel, String path, boolean followLinks) throws IOException {
        try {
            return getAttributes(channel, path, followLinks);
        } catch (@SuppressWarnings("unused") NoSuchFileException e) {
            return null;
        }
    }

    long getTotalSpace(SFTPPath path) throws IOException {
        try (Channel channel = channelPool.get()) {
            SftpStatVFS stat = channel.statVFS(normalizePath(path));
            // don't use stat.getSize because that uses kilobyte precision
            return stat.getFragmentSize() * stat.getBlocks();
        } catch (@SuppressWarnings("unused") UnsupportedOperationException e) {
            // statVFS is not available
            return Long.MAX_VALUE;
        }
    }

    long getUsableSpace(SFTPPath path) throws IOException {
        try (Channel channel = channelPool.get()) {
            SftpStatVFS stat = channel.statVFS(normalizePath(path));
            // don't use stat.getAvailForNonRoot because that uses kilobyte precision
            return stat.getFragmentSize() * stat.getAvailBlocks();
        } catch (@SuppressWarnings("unused") UnsupportedOperationException e) {
            // statVFS is not available
            return Long.MAX_VALUE;
        }
    }

    long getUnallocatedSpace(SFTPPath path) throws IOException {
        try (Channel channel = channelPool.get()) {
            SftpStatVFS stat = channel.statVFS(normalizePath(path));
            // don't use stat.getAvail because that uses kilobyte precision
            return stat.getFragmentSize() * stat.getFreeBlocks();
        } catch (@SuppressWarnings("unused") UnsupportedOperationException e) {
            // statVFS is not available
            return Long.MAX_VALUE;
        }
    }

    long getBlockSize(SFTPPath path) throws IOException {
        try (Channel channel = channelPool.get()) {
            // Propagate any UnsupportedOperationException, as that's allowed to be thrown
            SftpStatVFS stat = channel.statVFS(normalizePath(path));
            return stat.getBlockSize();
        }
    }
}
