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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import com.github.robtimus.filesystems.LinkOptionSupport;
import com.github.robtimus.filesystems.Messages;
import com.github.robtimus.filesystems.PathMatcherSupport;
import com.github.robtimus.filesystems.URISupport;
import com.github.robtimus.filesystems.attribute.PosixFilePermissionSupport;
import com.github.robtimus.filesystems.attribute.SimpleGroupPrincipal;
import com.github.robtimus.filesystems.attribute.SimpleUserPrincipal;
import com.github.robtimus.filesystems.sftp.SSHChannelPool.Channel;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.SftpATTRS;

/**
 * An FTP file system.
 *
 * @author Rob Spoor
 */
class SFTPFileSystem extends FileSystem {

    private static final String CURRENT_DIR = "."; //$NON-NLS-1$
    private static final String PARENT_DIR = ".."; //$NON-NLS-1$

    @SuppressWarnings("nls")
    private static final Set<String> SUPPORTED_FILE_ATTRIBUTE_VIEWS = Collections
            .unmodifiableSet(new HashSet<>(Arrays.asList("basic", "owner", "posix")));

    private final SFTPFileSystemProvider provider;
    private final Iterable<Path> rootDirectories;
    private final FileStore fileStore;
    private final Iterable<FileStore> fileStores;

    private final SSHChannelPool channelPool;
    private final URI uri;
    private final String defaultDirectory;

    private final boolean calculateActualTotalSpace;

    private final AtomicBoolean open = new AtomicBoolean(true);

    SFTPFileSystem(SFTPFileSystemProvider provider, URI uri, SFTPEnvironment env) throws IOException {
        this.provider = Objects.requireNonNull(provider);
        this.rootDirectories = Collections.<Path>singleton(new SFTPPath(this, "/")); //$NON-NLS-1$
        this.fileStore = new SFTPFileStore(this);
        this.fileStores = Collections.<FileStore>singleton(fileStore);

        this.channelPool = new SSHChannelPool(uri.getHost(), uri.getPort(), env);
        this.uri = Objects.requireNonNull(uri);

        try (Channel channel = channelPool.get()) {
            this.defaultDirectory = channel.pwd();
        }

        this.calculateActualTotalSpace = env.calculateActualTotalSpace();
    }

    @Override
    public FileSystemProvider provider() {
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
        return "/"; //$NON-NLS-1$
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return rootDirectories;
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return fileStores;
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return SUPPORTED_FILE_ATTRIBUTE_VIEWS;
    }

    @Override
    public Path getPath(String first, String... more) {
        StringBuilder sb = new StringBuilder(first);
        for (String s : more) {
            sb.append("/").append(s); //$NON-NLS-1$
        }
        return new SFTPPath(this, sb.toString());
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        final int index = syntaxAndPattern.indexOf(':');
        if (index == -1) {
            throw Messages.pathMatcher().syntaxNotFound(syntaxAndPattern);
        }
        String syntax = syntaxAndPattern.substring(0, index);
        String expression = syntaxAndPattern.substring(index + 1);

        final Pattern pattern = createPattern(syntax, expression);
        return new PathMatcher() {
            @Override
            public boolean matches(Path path) {
                return pattern.matcher(path.toString()).matches();
            }
        };
    }

    private Pattern createPattern(String syntax, String expression) {
        if ("glob".equals(syntax)) { //$NON-NLS-1$
            return PathMatcherSupport.toGlobPattern(expression);
        }
        if ("regex".equals(syntax)) { //$NON-NLS-1$
            return Pattern.compile(expression);
        }
        throw Messages.pathMatcher().unsupportedPathMatcherSyntax(syntax);
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
        SFTPPath absPath = toAbsolutePath(path).normalize();
        return toUri(absPath.path());
    }

    URI toUri(String path) {
        return URISupport.create(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), path, null, null);
    }

    SFTPPath toAbsolutePath(SFTPPath path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new SFTPPath(this, defaultDirectory + "/" + path.path()); //$NON-NLS-1$
    }

    SFTPPath toRealPath(SFTPPath path, LinkOption... options) throws IOException {
        boolean followLinks = LinkOptionSupport.followLinks(options);
        try (Channel channel = channelPool.get()) {
            return toRealPath(channel, path, followLinks).path;
        }
    }

    private SFTPPathAndAttributesPair toRealPath(Channel channel, SFTPPath path, boolean followLinks) throws IOException {
        SFTPPath absPath = toAbsolutePath(path).normalize();
        SftpATTRS attributes = getAttributes(channel, absPath, false);
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

    String toString(SFTPPath path) {
        return path.path();
    }

    InputStream newInputStream(SFTPPath path, OpenOption... options) throws IOException {
        OpenOptions openOptions = OpenOptions.forNewInputStream(options);

        try (Channel channel = channelPool.get()) {
            return newInputStream(channel, path, openOptions);
        }
    }

    private InputStream newInputStream(Channel channel, SFTPPath path, OpenOptions options) throws IOException {
        assert options.read;

        return channel.newInputStream(path.path(), options);
    }

    OutputStream newOutputStream(SFTPPath path, OpenOption... options) throws IOException {
        OpenOptions openOptions = OpenOptions.forNewOutputStream(options);

        try (Channel channel = channelPool.get()) {
            return newOutputStream(channel, path, false, openOptions).out;
        }
    }

    @SuppressWarnings("resource")
    private SFTPAttributesAndOutputStreamPair newOutputStream(Channel channel, SFTPPath path, boolean requireAttributes, OpenOptions options)
            throws IOException {

        // retrieve the attributes unless create is true and createNew is false, because then the file can be created
        SftpATTRS attributes = null;
        if (!options.create || options.createNew) {
            attributes = findAttributes(channel, path, false);
            if (attributes != null && attributes.isDir()) {
                throw Messages.fileSystemProvider().isDirectory(path.path());
            }
            if (!options.createNew && attributes == null) {
                throw new NoSuchFileException(path.path());
            } else if (options.createNew && attributes != null) {
                throw new FileAlreadyExistsException(path.path());
            }
        }
        // else the file can be created if necessary

        if (attributes == null && requireAttributes) {
            attributes = findAttributes(channel, path, false);
        }

        OutputStream out = channel.newOutputStream(path.path(), options);
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
            if (openOptions.read) {
                // use findAttributes instead of getAttributes, to let the opening of the stream provide the correct error message
                SftpATTRS attributes = findAttributes(channel, path, false);
                InputStream in = newInputStream(channel, path, openOptions);
                long size = attributes == null ? 0 : attributes.getSize();
                return FileSystemProviderSupport.createSeekableByteChannel(in, size);
            }

            // if append then we need the attributes, to find the initial position of the channel
            boolean requireAttributes = openOptions.append;
            SFTPAttributesAndOutputStreamPair outPair = newOutputStream(channel, path, requireAttributes, openOptions);
            long initialPosition = outPair.attributes == null ? 0 : outPair.attributes.getSize();
            return FileSystemProviderSupport.createSeekableByteChannel(outPair.out, initialPosition);
        }
    }

    DirectoryStream<Path> newDirectoryStream(final SFTPPath path, Filter<? super Path> filter) throws IOException {
        try (Channel channel = channelPool.get()) {
            List<LsEntry> entries = channel.listFiles(path.path());
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
                throw new NotDirectoryException(path.path());
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
            channel.mkdir(path.path());
        }
    }

    void delete(SFTPPath path) throws IOException {
        try (Channel channel = channelPool.get()) {
            SftpATTRS attributes = getAttributes(channel, path, false);
            boolean isDirectory = attributes.isDir();
            channel.delete(path.path(), isDirectory);
        }
    }

    SFTPPath readSymbolicLink(SFTPPath path) throws IOException {
        try (Channel channel = channelPool.get()) {
            return readSymbolicLink(channel, path);
        }
    }

    private SFTPPath readSymbolicLink(Channel channel, SFTPPath path) throws IOException {
        String link = channel.readSymbolicLink(path.path());
        return path.resolveSibling(link);
    }

    void copy(SFTPPath source, SFTPPath target, CopyOption... options) throws IOException {
        CopyOptions copyOptions = CopyOptions.forCopy(options);

        try (Channel channel = channelPool.get()) {
            // get the attributes to determine whether a directory needs to be created or a file needs to be copied
            // Files.copy specifies that for links, the final target must be copied
            SFTPPathAndAttributesPair sourcePair = toRealPath(channel, source, true);

            try {
                if (sourcePair.path.path().equals(toRealPath(channel, target, true).path.path())) {
                    // non-op, don't do a thing as specified by Files.copy
                    return;
                }
            } catch (@SuppressWarnings("unused") NoSuchFileException e) {
                // the target does not exist or either path is an invalid link, ignore the error and continue
            }

            SftpATTRS targetAttributes = findAttributes(channel, target, false);

            if (!copyOptions.replaceExisting && targetAttributes != null) {
                throw new FileAlreadyExistsException(target.path());
            }

            // replace existing or the target does not exist

            if (sourcePair.attributes.isDir()) {
                // create an directory, but only if target isn't an empty directory (but not a link to one)
                if (targetAttributes == null || !isEmptyDirectory(channel, target, targetAttributes)) {
                    channel.mkdir(target.path());
                }
            } else {
                try (Channel channel2 = channelPool.find()) {
                    if (channel2 == null) {
                        copyFile(channel, sourcePair, target);
                    } else {
                        OpenOptions inOptions = OpenOptions.forNewInputStream(copyOptions.toOpenOptions(StandardOpenOption.READ));
                        OpenOptions outOptions = OpenOptions
                                .forNewOutputStream(copyOptions.toOpenOptions(StandardOpenOption.WRITE, StandardOpenOption.CREATE));
                        try (InputStream in = channel.newInputStream(source.path(), inOptions)) {
                            channel2.storeFile(target.path(), in, outOptions.options);
                        }
                    }
                }
            }
        }
    }

    private boolean isEmptyDirectory(Channel channel, SFTPPath path, SftpATTRS attributes) throws IOException {
        return attributes.isDir() && channel.isEmptyDir(path.path());
    }

    @SuppressWarnings("resource")
    private void copyFile(Channel channel, SFTPPathAndAttributesPair sourcePair, SFTPPath target) throws IOException {
        LocalFile local = new LocalFile(sourcePair.attributes.getSize());
        channel.retrieveFile(sourcePair.path.path(), local);
        channel.storeFile(target.path(), local.getInputStream(), Collections.<OpenOption>emptySet());
    }

    // don't use ByteArrayOutputStream directly, because its toByteArray() method creates a copy; instead use it directly
    private static final class LocalFile extends ByteArrayOutputStream {

        private LocalFile(long size) {
            super((int) Math.max(0, Math.min(Integer.MAX_VALUE, size)));
        }

        private InputStream getInputStream() {
            return new ByteArrayInputStream(buf, 0, count);
        }
    }

    void move(SFTPPath source, SFTPPath target, CopyOption... options) throws IOException {
        CopyOptions copyOptions = CopyOptions.forMove(options);

        try (Channel channel = channelPool.get()) {
            try {
                if (isSameFile(channel, source, target)) {
                    // non-op, don't do a thing as specified by Files.move
                    return;
                }
            } catch (@SuppressWarnings("unused") NoSuchFileException e) {
                // the source or target does not exist or either path is an invalid link
                // call getAttributes to ensure the source file exists
                // ignore any error to target or if the source link is invalid
                getAttributes(channel, source, false);
            }

            if (toAbsolutePath(source).parentPath() == null) {
                // cannot move or rename the root
                throw new DirectoryNotEmptyException(source.path());
            }

            SftpATTRS targetAttributes = findAttributes(channel, target, false);
            if (copyOptions.replaceExisting && targetAttributes != null) {
                channel.delete(target.path(), targetAttributes.isDir());
            }

            channel.rename(source.path(), target.path());
        }
    }

    boolean isSameFile(SFTPPath path, SFTPPath path2) throws IOException {
        if (path.equals(path2)) {
            return true;
        }
        try (Channel channel = channelPool.get()) {
            return isSameFile(channel, path, path2);
        }
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
            getAttributes(channel, path, false);
        }
        String fileName = path.fileName();
        return !CURRENT_DIR.equals(fileName) && !PARENT_DIR.equals(fileName) && fileName.startsWith("."); //$NON-NLS-1$
    }

    FileStore getFileStore(SFTPPath path) throws IOException {
        // call getAttributes to check for existence
        try (Channel channel = channelPool.get()) {
            getAttributes(channel, path, false);
        }
        return fileStore;
    }

    void checkAccess(SFTPPath path, AccessMode... modes) throws IOException {
        try (Channel channel = channelPool.get()) {
            SftpATTRS attributes = getAttributes(channel, path, true);
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

    PosixFileAttributes readAttributes(SFTPPath path, LinkOption... options) throws IOException {
        boolean followLinks = LinkOptionSupport.followLinks(options);
        try (Channel channel = channelPool.get()) {
            SftpATTRS attributes = getAttributes(channel, path, followLinks);
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

    @SuppressWarnings("nls")
    private static final Set<String> BASIC_ATTRIBUTES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "basic:lastModifiedTime", "basic:lastAccessTime", "basic:creationTime", "basic:size",
            "basic:isRegularFile", "basic:isDirectory", "basic:isSymbolicLink", "basic:isOther", "basic:fileKey")));

    @SuppressWarnings("nls")
    private static final Set<String> OWNER_ATTRIBUTES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "owner:owner")));

    @SuppressWarnings("nls")
    private static final Set<String> POSIX_ATTRIBUTES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "posix:lastModifiedTime", "posix:lastAccessTime", "posix:creationTime", "posix:size",
            "posix:isRegularFile", "posix:isDirectory", "posix:isSymbolicLink", "posix:isOther", "posix:fileKey",
            "posix:owner", "posix:group", "posix:permissions")));

    Map<String, Object> readAttributes(SFTPPath path, String attributes, LinkOption... options) throws IOException {

        String view;
        int pos = attributes.indexOf(':');
        if (pos == -1) {
            view = "basic"; //$NON-NLS-1$
            attributes = "basic:" + attributes; //$NON-NLS-1$
        } else {
            view = attributes.substring(0, pos);
        }
        if (!SUPPORTED_FILE_ATTRIBUTE_VIEWS.contains(view)) {
            throw Messages.fileSystemProvider().unsupportedFileAttributeView(view);
        }

        Set<String> allowedAttributes;
        if (attributes.startsWith("basic:")) { //$NON-NLS-1$
            allowedAttributes = BASIC_ATTRIBUTES;
        } else if (attributes.startsWith("owner:")) { //$NON-NLS-1$
            allowedAttributes = OWNER_ATTRIBUTES;
        } else if (attributes.startsWith("posix:")) { //$NON-NLS-1$
            allowedAttributes = POSIX_ATTRIBUTES;
        } else {
            // should not occur
            throw Messages.fileSystemProvider().unsupportedFileAttributeView(attributes.substring(0, attributes.indexOf(':')));
        }

        Map<String, Object> result = getAttributeMap(attributes, allowedAttributes);

        PosixFileAttributes posixAttributes = readAttributes(path, options);

        for (Map.Entry<String, Object> entry : result.entrySet()) {
            switch (entry.getKey()) {
            case "basic:lastModifiedTime": //$NON-NLS-1$
            case "posix:lastModifiedTime": //$NON-NLS-1$
                entry.setValue(posixAttributes.lastModifiedTime());
                break;
            case "basic:lastAccessTime": //$NON-NLS-1$
            case "posix:lastAccessTime": //$NON-NLS-1$
                entry.setValue(posixAttributes.lastAccessTime());
                break;
            case "basic:creationTime": //$NON-NLS-1$
            case "posix:creationTime": //$NON-NLS-1$
                entry.setValue(posixAttributes.creationTime());
                break;
            case "basic:size": //$NON-NLS-1$
            case "posix:size": //$NON-NLS-1$
                entry.setValue(posixAttributes.size());
                break;
            case "basic:isRegularFile": //$NON-NLS-1$
            case "posix:isRegularFile": //$NON-NLS-1$
                entry.setValue(posixAttributes.isRegularFile());
                break;
            case "basic:isDirectory": //$NON-NLS-1$
            case "posix:isDirectory": //$NON-NLS-1$
                entry.setValue(posixAttributes.isDirectory());
                break;
            case "basic:isSymbolicLink": //$NON-NLS-1$
            case "posix:isSymbolicLink": //$NON-NLS-1$
                entry.setValue(posixAttributes.isSymbolicLink());
                break;
            case "basic:isOther": //$NON-NLS-1$
            case "posix:isOther": //$NON-NLS-1$
                entry.setValue(posixAttributes.isOther());
                break;
            case "basic:fileKey": //$NON-NLS-1$
            case "posix:fileKey": //$NON-NLS-1$
                entry.setValue(posixAttributes.fileKey());
                break;
            case "owner:owner": //$NON-NLS-1$
            case "posix:owner": //$NON-NLS-1$
                entry.setValue(posixAttributes.owner());
                break;
            case "posix:group": //$NON-NLS-1$
                entry.setValue(posixAttributes.group());
                break;
            case "posix:permissions": //$NON-NLS-1$
                entry.setValue(posixAttributes.permissions());
                break;
            default:
                // should not occur
                throw new IllegalStateException("unexpected attribute name: " + entry.getKey()); //$NON-NLS-1$
            }
        }
        return result;
    }

    private Map<String, Object> getAttributeMap(String attributes, Set<String> allowedAttributes) {
        int indexOfColon = attributes.indexOf(':');
        String prefix = attributes.substring(0, indexOfColon + 1);
        attributes = attributes.substring(indexOfColon + 1);

        String[] attributeList = attributes.split(","); //$NON-NLS-1$
        Map<String, Object> result = new HashMap<>(allowedAttributes.size());

        for (String attribute : attributeList) {
            String prefixedAttribute = prefix + attribute;
            if (allowedAttributes.contains(prefixedAttribute)) {
                result.put(prefixedAttribute, null);
            } else if ("*".equals(attribute)) { //$NON-NLS-1$
                for (String s : allowedAttributes) {
                    result.put(s, null);
                }
            } else {
                throw Messages.fileSystemProvider().unsupportedFileAttribute(attribute);
            }
        }
        return result;
    }

    void setOwner(SFTPPath path, UserPrincipal owner) throws IOException {
        setOwner(path, owner, false);
    }

    private void setOwner(SFTPPath path, UserPrincipal owner, boolean followLinks) throws IOException {
        try {
            int uid = Integer.parseInt(owner.getName());
            try (Channel channel = channelPool.get()) {
                if (followLinks) {
                    path = toRealPath(channel, path, followLinks).path;
                }
                channel.chown(path.path(), uid);
            }
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
    }

    void setGroup(SFTPPath path, GroupPrincipal group) throws IOException {
        setGroup(path, group, false);
    }

    private void setGroup(SFTPPath path, GroupPrincipal group, boolean followLinks) throws IOException {
        try {
            int gid = Integer.parseInt(group.getName());
            try (Channel channel = channelPool.get()) {
                if (followLinks) {
                    path = toRealPath(channel, path, followLinks).path;
                }
                channel.chgrp(path.path(), gid);
            }
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
    }

    void setPermissions(SFTPPath path, Set<PosixFilePermission> permissions) throws IOException {
        setPermissions(path, permissions, false);
    }

    private void setPermissions(SFTPPath path, Set<PosixFilePermission> permissions, boolean followLinks) throws IOException {
        try (Channel channel = channelPool.get()) {
            if (followLinks) {
                path = toRealPath(channel, path, followLinks).path;
            }
            channel.chmod(path.path(), PosixFilePermissionSupport.toMask(permissions));
        }
    }

    void setTimes(SFTPPath path, FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        if (lastAccessTime != null) {
            throw new IOException(Messages.fileSystemProvider().unsupportedFileAttribute("lastAccessTime")); //$NON-NLS-1$
        }
        if (createTime != null) {
            throw new IOException(Messages.fileSystemProvider().unsupportedFileAttribute("createAccessTime")); //$NON-NLS-1$
        }
        if (lastModifiedTime != null) {
            setLastModifiedTime(path, lastModifiedTime, false);
        }
    }

    void setLastModifiedTime(SFTPPath path, FileTime lastModifiedTime, boolean followLinks) throws IOException {
        try (Channel channel = channelPool.get()) {
            if (followLinks) {
                path = toRealPath(channel, path, followLinks).path;
            }
            // times are in seconds
            channel.setMtime(path.path(), lastModifiedTime.to(TimeUnit.SECONDS));
        }
    }

    void setAttribute(SFTPPath path, String attribute, Object value, LinkOption... options) throws IOException {
        String view;
        int pos = attribute.indexOf(':');
        if (pos == -1) {
            view = "basic"; //$NON-NLS-1$
            attribute = "basic:" + attribute; //$NON-NLS-1$
        } else {
            view = attribute.substring(0, pos);
        }
        if (!"basic".equals(view) && !"owner".equals(view) && !"posix".equals(view)) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            throw Messages.fileSystemProvider().unsupportedFileAttributeView(view);
        }

        boolean followLinks = LinkOptionSupport.followLinks(options);

        switch (attribute) {
        case "basic:lastModifiedTime": //$NON-NLS-1$
        case "posix:lastModifiedTime": //$NON-NLS-1$
            setLastModifiedTime(path, (FileTime) value, followLinks);
            break;
        case "owner:owner": //$NON-NLS-1$
        case "posix:owner": //$NON-NLS-1$
            setOwner(path, (UserPrincipal) value, followLinks);
            break;
        case "posix:group": //$NON-NLS-1$
            setGroup(path, (GroupPrincipal) value, followLinks);
            break;
        case "posix:permissions": //$NON-NLS-1$
            @SuppressWarnings("unchecked")
            Set<PosixFilePermission> permissions = (Set<PosixFilePermission>) value;
            setPermissions(path, permissions, followLinks);
            break;
        default:
            throw Messages.fileSystemProvider().unsupportedFileAttribute(attribute);
        }
    }

    private SftpATTRS getAttributes(Channel channel, SFTPPath path, boolean followLinks) throws IOException {
        return channel.readAttributes(path.path(), followLinks);
    }

    private SftpATTRS findAttributes(Channel channel, SFTPPath path, boolean followLinks) throws IOException {
        try {
            return getAttributes(channel, path, followLinks);
        } catch (@SuppressWarnings("unused") NoSuchFileException e) {
            return null;
        }
    }

    long getTotalSpace() throws IOException {
        // FTPClient does not support the total size easily, so only calculate if explicitly requested
        if (calculateActualTotalSpace) {
            try (Channel channel = channelPool.get()) {
                return getTotalSize(channel, "/"); //$NON-NLS-1$
            }
        }
        return Long.MAX_VALUE;
    }

    long getUsableSpace() {
        // FTPClient does not support the total size
        return Long.MAX_VALUE;
    }

    long getUnallocatedSpace() {
        // FTPClient does not support the total size
        return Long.MAX_VALUE;
    }

    private long getTotalSize(Channel channel, String path) throws IOException {
        long size = 0;
        List<LsEntry> entries = channel.listFiles(path);
        for (LsEntry entry : entries) {
            String filename = entry.getFilename();
            if (!CURRENT_DIR.equals(filename) && !PARENT_DIR.equals(filename)) {
                SftpATTRS attributes = entry.getAttrs();
                if (attributes.isDir()) {
                    String newPath = "/".equals(path) ? "/" + filename : path + "/" + filename; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    size += getTotalSize(channel, newPath);
                } else {
                    size += attributes.getSize();
                }
            }
        }
        return size;
    }
}
