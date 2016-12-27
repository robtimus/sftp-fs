# sftp-fs

The `sftp-fs` library provides support for SFTP NIO.2 file systems, allowing SFTP servers to be accessed in a similar way to local file systems.

## Creating file systems

If the SFTP file system library is available on the class path, it will register a [FileSystemProvider](https://docs.oracle.com/javase/8/docs/api/java/nio/file/spi/FileSystemProvider.html) for scheme `sftp`. This allows you to create SFTP file systems using the `newFileSystem` methods of class [FileSystems](https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileSystems.html). You can use class [SFTPEnvironment](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPEnvironment.html) to help create the environment maps for those methods:

    SFTPEnvironment env = new SFTPEnvironment()
            .withUsername(username)
            .withUserInfo(userInfo);
    FileSystem fs = FileSystems.newFileSystem(URI.create("sftp://example.org"), env);

Note that, for security reasons, it's not allowed to pass the credentials as part of the URI when creating a file system. It must be passed through the environment, as shown above.

### Providing credentials

There are a few ways to provide credentials. The easiest way is to call [withUserInfo](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPEnvironment.html#withUserInfo-com.jcraft.jsch.UserInfo-) on an [SFTPEnvironment](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPEnvironment.html) instance with a [UserInfo](https://epaul.github.io/jsch-documentation/javadoc/com/jcraft/jsch/UserInfo.html) object to provide the password, and if necessary the passphrase. The `sftp-fs` library provides class [SimpleUserInfo](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SimpleUserInfo.html) to provide a fixed password and passphrase. Note that any prompt will be answered with "no". That means that you will most likely also need to set a [HostKeyRepository](https://epaul.github.io/jsch-documentation/javadoc/com/jcraft/jsch/HostKeyRepository.html) by calling [withHostKeyRepository](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPEnvironment.html#withHostKeyRepository-com.jcraft.jsch.HostKeyRepository-) on an [SFTPEnvironment](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPEnvironment.html) instance to verify the host.

It's also possible to use other authentication methods using an [IdentityRepository](https://epaul.github.io/jsch-documentation/javadoc/com/jcraft/jsch/IdentityRepository.html), which can be set by calling [withIdentityRepository](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPEnvironment.html#withIdentityRepository-com.jcraft.jsch.IdentityRepository-) on an [SFTPEnvironment](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPEnvironment.html) instance. This may require additional dependencies. For example, to use pageant, you will need the following additional dependencies:

    <dependency>
      <groupId>com.jcraft</groupId>
      <artifactId>jsch.agentproxy.jsch</artifactId>
      <version>0.0.9</version>
    </dependency>
    <dependency>
      <groupId>com.jcraft</groupId>
      <artifactId>jsch.agentproxy.pageant</artifactId>
      <version>0.0.9</version>
    </dependency>

The following code snippet shows how you can use pageant to authenticate:

    SFTPEnvironment env = new SFTPEnvironment()
            .withUsername(username)
            .withIdentityRepository(new RemoteIdentityRepository(new PageantConnector()));
    FileSystem fs = FileSystems.newFileSystem(URI.create("sftp://example.org"), env);

## Creating paths

After a file system has been created, [Paths](https://docs.oracle.com/javase/8/docs/api/java/nio/file/Path.html) can be created through the file system itself using its [getPath](https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileSystem.html#getPath-java.lang.String-java.lang.String...-) method. As long as the file system is not closed, it's also possible to use [Paths.get](https://docs.oracle.com/javase/8/docs/api/java/nio/file/Paths.html#get-java.net.URI-). Note that if the file system was created with credentials, the username must be part of the URL. For instance:

    // created without credentials
    Path path1 = Paths.get(URI.create("sftp://example.org"));
    // created with credentials
    Path path2 = Paths.get(URI.create("sftp://username@example.org"));

If the username in the URI does not match the username used to create the file system, this will cause a [FileSystemNotFoundException](https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileSystemNotFoundException.html) to be thrown.

## Attributes

### File attributes

SFTP file systems fully support read-access to the following attributes:

* Attributes defined in [BasicFileAttributeView](https://docs.oracle.com/javase/8/docs/api/java/nio/file/attribute/BasicFileAttributeView.html) and [BasicFileAttributes](https://docs.oracle.com/javase/8/docs/api/java/nio/file/attribute/BasicFileAttributes.html), available both with and without prefixes `basic:` or `posix:`.
* Attributes defined in [FileOwnerAttributeView](https://docs.oracle.com/javase/8/docs/api/java/nio/file/attribute/FileOwnerAttributeView.html), available with prefixes `owner:` or `posix:`.
* Attributes defined in [PosixFileAttributeView](https://docs.oracle.com/javase/8/docs/api/java/nio/file/attribute/PosixFileAttributeView.html) and [PosixFileAttributes](https://docs.oracle.com/javase/8/docs/api/java/nio/file/attribute/PosixFileAttributes.html), available with prefix `posix:`.

It's not possible to set the last access time or creation time, either through one of the file attribute views or through a file system. Attempting to do so will result in an [UnsupportedOperationException](https://docs.oracle.com/javase/8/docs/api/java/lang/UnsupportedOperationException.html). All other attributes are supported, although when setting the owner or group the name must be the UID/GID.

### File store attributes

When calling [getAttribute](https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileStore.html#getAttribute-java.lang.String-) on a file store, the following attributes are supported:

* `totalSpace`: returns the same value as the [getTotalSpace](https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileStore.html#getTotalSpace--) method.
* `usableSpace`: returns the same value as the [getUsableSpace](https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileStore.html#getUsableSpace--) method.
* `unallocatedSpace`: returns the same value as the [getUnallocatedSpace](https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileStore.html#getUnallocatedSpace--) method.

Because SFTP servers do not (often) return these values, by default these methods will all return `Long.MAX_VALUE`. For `totalSpace`, it's possible to return the actual total space, by calling [withActualTotalSpaceCalculation](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPEnvironment.html#withActualTotalSpaceCalculation-boolean-)
 on an [SFTPEnvironment](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPEnvironment.html) instance before using it to create the file system. Beware that this call will perform multiple calls to the SFTP server. It's therefore not advised to do this.

There is no support for [FileStoreAttributeView](https://docs.oracle.com/javase/8/docs/api/java/nio/file/attribute/FileStoreAttributeView.html). Calling [getFileStoreAttributeView](https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileStore.html#getFileStoreAttributeView-java.lang.Class-) on a file store will simply return `null`.

## Error handling

Not all SFTP servers support the same set of error codes. Because of this, most methods may not throw the correct exception ([NoSuchFileException](https://docs.oracle.com/javase/8/docs/api/java/nio/file/NoSuchFileException.html), [AccessDeniedException](https://docs.oracle.com/javase/8/docs/api/java/nio/file/AccessDeniedException.html), etc).

To allow this behaviour to be modified, class [SFTPEnvironment](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPEnvironment.html) has method [withFileSystemExceptionFactory](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPEnvironment.html#withFileSystemExceptionFactory-com.github.robtimus.filesystems.sftp.FileSystemExceptionFactory-) that allows you to specify a custom [FileSystemExceptionFactory](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/FileSystemExceptionFactory.html) implementation which will be used to create exceptions based on the reply code and string of the command that triggered the error. By default, an instance of class [DefaultFileSystemExceptionFactory](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/DefaultFileSystemExceptionFactory.html) is used.

## Thread safety

The SFTP protocol is fundamentally not thread safe. To overcome this limitation, SFTP file systems maintain multiple connections to SFTP servers. The number of connections determines the number of concurrent operations that can be executed. If all connections are busy, a new operation will block until a connection becomes available. Class [SFTPEnvironment](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPEnvironment.html) has method [withClientConnectionCount](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPEnvironment.html#withClientConnectionCount-int-) that allows you to specify the number of connections to use. If no connection count is explicitly set, the default will be 5.

When a stream or channel is opened for reading or writing, the connection will block because it will wait for the download or upload to finish. This will not occur until the stream or channel is closed. It is therefore advised to close streams and channels as soon as possible.

## Connection management

Because SFTP file systems use multiple connections to an SFTP server, it's possible that one or more of these connections become stale. Class [SFTPFileSystemProvider](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPFileSystemProvider.html) has static method [keepAlive](https://robtimus.github.io/sftp-fs/apidocs/com/github/robtimus/filesystems/sftp/SFTPFileSystemProvider.html#keepAlive-java.nio.file.FileSystem-) that, if given an instance of an SFTP file system, will send a keep-alive signal over each of its idle connections. You should ensure that this method is called on a regular interval.

## Limitations

SFTP file systems knows the following limitations:

* All paths use `/` as separator. `/` is not allowed inside file or directory names.
* File attributes cannot be set when creating files or directories.
* Symbolic links can be read and traversed, but not created.
* There is no support for hard links.
* Files can be marked as executable if the SFTP server indicates it is. That does not mean the file can be executed in the local JVM.
* [SeekableByteChannel](https://docs.oracle.com/javase/8/docs/api/java/nio/channels/SeekableByteChannel.html) is supported because it's used by [Files.createFile](https://docs.oracle.com/javase/8/docs/api/java/nio/file/Files.html#createFile-java.nio.file.Path-java.nio.file.attribute.FileAttribute...-). However, these channels do not support seeking specific positions or truncating.
* When copying files, an SFTP file system will attempt to claim 2 connections, one for downloading and one for uploading. To prevent deadlocks, if only one connection is available, files are downloaded to memory before being uploaded over the same connection. This means that copying large files may cause memory issues.
* There is no support for [UserPrincipalLookupService](https://docs.oracle.com/javase/8/docs/api/java/nio/file/attribute/UserPrincipalLookupService.html).
* There is no support for [WatchService](https://docs.oracle.com/javase/8/docs/api/java/nio/file/WatchService.html).
