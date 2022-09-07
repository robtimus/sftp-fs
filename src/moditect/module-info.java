module com.github.robtimus.filesystems.sftp {
    requires com.github.robtimus.filesystems;
    requires transitive com.jcraft.jsch;
    requires static org.slf4j;

    exports com.github.robtimus.filesystems.sftp;

    provides java.nio.file.spi.FileSystemProvider with com.github.robtimus.filesystems.sftp.SFTPFileSystemProvider;
}
