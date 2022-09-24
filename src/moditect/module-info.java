module com.github.robtimus.filesystems.sftp {
    requires com.github.robtimus.filesystems;
    requires transitive com.jcraft.jsch;
    requires com.github.robtimus.pool;

    exports com.github.robtimus.filesystems.sftp;

    provides java.nio.file.spi.FileSystemProvider with com.github.robtimus.filesystems.sftp.SFTPFileSystemProvider;
}
