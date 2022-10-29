/*
 * SFTPFileSystemProvider.java
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
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import com.github.robtimus.filesystems.FileSystemMap;
import com.github.robtimus.filesystems.Messages;
import com.github.robtimus.filesystems.URISupport;

/**
 * A provider for SFTP file systems.
 *
 * @author Rob Spoor
 */
public class SFTPFileSystemProvider extends FileSystemProvider {

    private final FileSystemMap<SFTPFileSystem> fileSystems = new FileSystemMap<>(this::createFileSystem);

    /**
     * Returns the URI scheme that identifies this provider: {@code sftp}.
     */
    @Override
    public String getScheme() {
        return "sftp"; //$NON-NLS-1$
    }

    private SFTPFileSystem createFileSystem(URI uri, Map<String, ?> env) throws IOException {
        SFTPEnvironment environment = (SFTPEnvironment) env;
        return new SFTPFileSystem(this, uri, environment);
    }

    /**
     * Constructs a new {@code FileSystem} object identified by a URI.
     * <p>
     * The URI must have a {@link URI#getScheme() scheme} equal to {@link #getScheme()}, and no {@link URI#getUserInfo() user information},
     * {@link URI#getPath() path}, {@link URI#getQuery() query} or {@link URI#getFragment() fragment}. Authentication credentials must be set through
     * the given environment map, preferably through {@link SFTPEnvironment}.
     * <p>
     * This provider allows multiple file systems per host, but only one file system per user on a host.
     * Once a file system is {@link FileSystem#close() closed}, this provider allows a new file system to be created with the same URI and credentials
     * as the closed file system.
     */
    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        // user info must come from the environment map
        checkURI(uri, false, false);

        SFTPEnvironment environment = SFTPEnvironment.copy(env);

        String username = environment.getUsername();
        URI normalizedURI = normalizeWithUsername(uri, username);
        return fileSystems.add(normalizedURI, environment);
    }

    /**
     * Returns an existing {@code FileSystem} created by this provider.
     * <p>
     * The URI must have a {@link URI#getScheme() scheme} equal to {@link #getScheme()}, and no {@link URI#getPath() path},
     * {@link URI#getQuery() query} or {@link URI#getFragment() fragment}. Because the original credentials were provided through an environment map,
     * the URI can contain {@link URI#getUserInfo() user information}, although this should not contain a password for security reasons.
     * <p>
     * Once a file system is {@link FileSystem#close() closed}, this provided will throw a {@link FileSystemNotFoundException}.
     */
    @Override
    public FileSystem getFileSystem(URI uri) {
        checkURI(uri, true, false);

        return getExistingFileSystem(uri);
    }

    /**
     * Return a {@code Path} object by converting the given {@link URI}. The resulting {@code Path} is associated with a {@link FileSystem} that
     * already exists. This method does not support constructing {@code FileSystem}s automatically.
     * <p>
     * The URI must have a {@link URI#getScheme() scheme} equal to {@link #getScheme()}, and no {@link URI#getQuery() query} or
     * {@link URI#getFragment() fragment}. Because the original credentials were provided through an environment map,
     * the URI can contain {@link URI#getUserInfo() user information}, although this should not contain a password for security reasons.
     */
    @Override
    @SuppressWarnings("resource")
    public Path getPath(URI uri) {
        checkURI(uri, true, true);

        SFTPFileSystem fs = getExistingFileSystem(uri);
        return fs.getPath(uri.getPath());
    }

    private SFTPFileSystem getExistingFileSystem(URI uri) {
        URI normalizedURI = normalizeWithoutPassword(uri);
        return fileSystems.get(normalizedURI);
    }

    private void checkURI(URI uri, boolean allowUserInfo, boolean allowPath) {
        if (!uri.isAbsolute()) {
            throw Messages.uri().notAbsolute(uri);
        }
        if (!getScheme().equalsIgnoreCase(uri.getScheme())) {
            throw Messages.uri().invalidScheme(uri, getScheme());
        }
        if (!allowUserInfo && uri.getUserInfo() != null && !uri.getUserInfo().isEmpty()) {
            throw Messages.uri().hasUserInfo(uri);
        }
        if (uri.isOpaque()) {
            throw Messages.uri().notHierarchical(uri);
        }
        if (!allowPath && uri.getPath() != null && !uri.getPath().isEmpty()) {
            throw Messages.uri().hasPath(uri);
        }
        if (uri.getQuery() != null && !uri.getQuery().isEmpty()) {
            throw Messages.uri().hasQuery(uri);
        }
        if (uri.getFragment() != null && !uri.getFragment().isEmpty()) {
            throw Messages.uri().hasFragment(uri);
        }
    }

    void removeFileSystem(URI uri) {
        URI normalizedURI = normalizeWithoutPassword(uri);
        fileSystems.remove(normalizedURI);
    }

    static URI normalizeWithoutPassword(URI uri) {
        String userInfo = uri.getUserInfo();
        if (userInfo == null && uri.getPath() == null && uri.getQuery() == null && uri.getFragment() == null) {
            // nothing to normalize, return the URI
            return uri;
        }
        String username = null;
        if (userInfo != null) {
            int index = userInfo.indexOf(':');
            username = index == -1 ? userInfo : userInfo.substring(0, index);
        }
        // no path, query or fragment
        return URISupport.create(uri.getScheme(), username, uri.getHost(), uri.getPort(), null, null, null);
    }

    static URI normalizeWithUsername(URI uri, String username) {
        if (username == null && uri.getUserInfo() == null && uri.getPath() == null && uri.getQuery() == null && uri.getFragment() == null) {
            // nothing to normalize or add, return the URI
            return uri;
        }
        // no path, query or fragment
        return URISupport.create(uri.getScheme(), username, uri.getHost(), uri.getPort(), null, null, null);
    }

    /**
     * Opens a file, returning an input stream to read from the file.
     * This method works in exactly the manner specified by the {@link Files#newInputStream(Path, OpenOption...)} method.
     * <p>
     * Note: while the returned input stream is not closed, the path's file system will have one available connection fewer.
     * It is therefore essential that the input stream is closed as soon as possible.
     */
    @Override
    public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        return toSFTPPath(path).newInputStream(options);
    }

    /**
     * Opens or creates a file, returning an output stream that may be used to write bytes to the file.
     * This method works in exactly the manner specified by the {@link Files#newOutputStream(Path, OpenOption...)} method.
     * <p>
     * Note: while the returned output stream is not closed, the path's file system will have one available connection fewer.
     * It is therefore essential that the output stream is closed as soon as possible.
     */
    @Override
    public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        return toSFTPPath(path).newOutputStream(options);
    }

    /**
     * Opens or creates a file, returning a seekable byte channel to access the file.
     * This method works in exactly the manner specified by the {@link Files#newByteChannel(Path, Set, FileAttribute...)} method.
     * <p>
     * This method does not support any file attributes to be set. If any file attributes are given, an {@link UnsupportedOperationException} will be
     * thrown.
     * <p>
     * Note: while the returned channel is not closed, the path's file system will have one available connection fewer.
     * It is therefore essential that the channel is closed as soon as possible.
     */
    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        return toSFTPPath(path).newByteChannel(options, attrs);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter) throws IOException {
        return toSFTPPath(dir).newDirectoryStream(filter);
    }

    /**
     * Creates a new directory.
     * This method works in exactly the manner specified by the {@link Files#createDirectory(Path, FileAttribute...)} method.
     * <p>
     * This method does not support any file attributes to be set. If any file attributes are given, an {@link UnsupportedOperationException} will be
     * thrown.
     */
    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        toSFTPPath(dir).createDirectory(attrs);
    }

    @Override
    public void delete(Path path) throws IOException {
        toSFTPPath(path).delete();
    }

    @Override
    public Path readSymbolicLink(Path link) throws IOException {
        return toSFTPPath(link).readSymbolicLink();
    }

    /**
     * Copy a file to a target file.
     * This method works in exactly the manner specified by the {@link Files#copy(Path, Path, CopyOption...)} method except that both the source and
     * target paths must be associated with this provider.
     * <p>
     * Most of the standard copy options are supported. {@link StandardCopyOption#COPY_ATTRIBUTES} and {@link StandardCopyOption#ATOMIC_MOVE} are not
     * supported though.
     */
    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        toSFTPPath(source).copy(toSFTPPath(target), options);
    }

    /**
     * Move or rename a file to a target file.
     * This method works in exactly the manner specified by the {@link Files#move(Path, Path, CopyOption...)} method except that both the source and
     * target paths must be associated with this provider.
     * <p>
     * Most of the standard copy options are supported. {@link StandardCopyOption#COPY_ATTRIBUTES} is not supported though.
     * {@link StandardCopyOption#ATOMIC_MOVE} is only supported if the paths have the same file system.
     */
    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        toSFTPPath(source).move(toSFTPPath(target), options);
    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        return toSFTPPath(path).isSameFile(path2);
    }

    @Override
    public boolean isHidden(Path path) throws IOException {
        return toSFTPPath(path).isHidden();
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        return toSFTPPath(path).getFileStore();
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        toSFTPPath(path).checkAccess(modes);
    }

    /**
     * Returns a file attribute view of a given type.
     * This method works in exactly the manner specified by the {@link Files#getFileAttributeView(Path, Class, LinkOption...)} method.
     * <p>
     * This provider supports {@link BasicFileAttributeView}, {@link FileOwnerAttributeView} and {@link PosixFileAttributeView}.
     * All other classes will result in a {@code null} return value.
     * <p>
     * Note: if the type is {@link BasicFileAttributeView} or a sub type, the last access time and creation time must be {@code null} when calling
     * {@link BasicFileAttributeView#setTimes(FileTime, FileTime, FileTime)}, otherwise an exception will be thrown.
     * When setting the owner or group for the path, the name must be the UID/GID of the owner/group.
     */
    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        Objects.requireNonNull(type);
        if (type == BasicFileAttributeView.class) {
            return type.cast(new AttributeView("basic", toSFTPPath(path))); //$NON-NLS-1$
        }
        if (type == FileOwnerAttributeView.class) {
            return type.cast(new AttributeView("owner", toSFTPPath(path))); //$NON-NLS-1$
        }
        if (type == PosixFileAttributeView.class) {
            return type.cast(new AttributeView("posix", toSFTPPath(path))); //$NON-NLS-1$
        }
        return null;
    }

    private static final class AttributeView implements PosixFileAttributeView {

        private final String name;
        private final SFTPPath path;

        private AttributeView(String name, SFTPPath path) {
            this.name = Objects.requireNonNull(name);
            this.path = Objects.requireNonNull(path);
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
            return path.readAttributes();
        }

        @Override
        public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
            path.setTimes(lastModifiedTime, lastAccessTime, createTime);
        }

        @Override
        public void setOwner(UserPrincipal owner) throws IOException {
            path.setOwner(owner);
        }

        @Override
        public void setGroup(GroupPrincipal group) throws IOException {
            path.setGroup(group);
        }

        @Override
        public void setPermissions(Set<PosixFilePermission> perms) throws IOException {
            path.setPermissions(perms);
        }
    }

    /**
     * Reads a file's attributes as a bulk operation.
     * This method works in exactly the manner specified by the {@link Files#readAttributes(Path, Class, LinkOption...)} method.
     * <p>
     * This provider supports {@link BasicFileAttributes} and {@link PosixFileAttributes} (there is no {@code FileOwnerFileAttributes}).
     * All other classes will result in an {@link UnsupportedOperationException} to be thrown.
     */
    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        if (type == BasicFileAttributes.class || type == PosixFileAttributes.class) {
            return type.cast(toSFTPPath(path).readAttributes(options));
        }
        throw Messages.fileSystemProvider().unsupportedFileAttributesType(type);
    }

    /**
     * Reads a set of file attributes as a bulk operation.
     * This method works in exactly the manner specified by the {@link Files#readAttributes(Path, String, LinkOption...)} method.
     * <p>
     * This provider supports views {@code basic}, {@code owner} and {@code posix}, where {@code basic} will be used if no view is given.
     * All other views will result in an {@link UnsupportedOperationException} to be thrown.
     */
    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        return toSFTPPath(path).readAttributes(attributes, options);
    }

    /**
     * Sets the value of a file attribute.
     * This method works in exactly the manner specified by the {@link Files#setAttribute(Path, String, Object, LinkOption...)} method.
     * <p>
     * This provider supports views {@code basic}, {@code owner} and {@code posix}, where {@code basic} will be used if no view is given.
     * All other views will result in an {@link UnsupportedOperationException} to be thrown.
     * <p>
     * Note: updating the last access time or creation time is not supported.
     * When setting the owner or group for the path, the name must be the UID/GID of the owner/group.
     */
    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        toSFTPPath(path).setAttribute(attribute, value, options);
    }

    private static SFTPPath toSFTPPath(Path path) {
        Objects.requireNonNull(path);
        if (path instanceof SFTPPath) {
            return (SFTPPath) path;
        }
        throw new ProviderMismatchException();
    }

    /**
     * Send a keep-alive signal for an SFTP file system.
     *
     * @param fs The SFTP file system to send a keep-alive signal for.
     * @throws ProviderMismatchException If the given file system is not an SFTP file system (not created by an {@code SFTPFileSystemProvider}).
     * @throws IOException If an I/O error occurred.
     */
    public static void keepAlive(FileSystem fs) throws IOException {
        if (fs instanceof SFTPFileSystem) {
            ((SFTPFileSystem) fs).keepAlive();
            return;
        }
        throw new ProviderMismatchException();
    }
}
