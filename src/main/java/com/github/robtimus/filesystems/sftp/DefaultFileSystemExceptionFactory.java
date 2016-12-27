/*
 * DefaultFileSystemExceptionFactory.java
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

import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotLinkException;
import java.nio.file.OpenOption;
import java.util.Collection;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;

/**
 * A default {@link FileSystemExceptionFactory} that will return a {@link NoSuchFileException} if the {@link SftpException#id id} of the
 * {@link SftpException} is {@link ChannelSftp#SSH_FX_NO_SUCH_FILE}, an {@link AccessDeniedException} if it's
 * {@link ChannelSftp#SSH_FX_PERMISSION_DENIED}, or a {@link FileSystemException} otherwise, unless specified otherwise.
 *
 * @author Rob Spoor
 */
public class DefaultFileSystemExceptionFactory implements FileSystemExceptionFactory {

    static final DefaultFileSystemExceptionFactory INSTANCE = new DefaultFileSystemExceptionFactory();

    @Override
    public FileSystemException createGetFileException(String file, SftpException exception) {
        return asFileSystemException(file, null, exception);
    }

    /**
     * {@inheritDoc}
     * <p>
     * If the {@link SftpException#id id} of the {@link SftpException} is not {@link ChannelSftp#SSH_FX_NO_SUCH_FILE} or
     * {@link ChannelSftp#SSH_FX_PERMISSION_DENIED}, this default implementation does not return a {@link FileSystemException}, but a
     * {@link NotLinkException} instead.
     */
    @Override
    public FileSystemException createReadLinkException(String link, SftpException exception) {
        if (exception.id == ChannelSftp.SSH_FX_NO_SUCH_FILE || exception.id == ChannelSftp.SSH_FX_PERMISSION_DENIED) {
            return asFileSystemException(link, null, exception);
        }
        final FileSystemException result = new NotLinkException(link, null, exception.getMessage());
        result.initCause(exception);
        return result;
    }

    @Override
    public FileSystemException createListFilesException(String directory, SftpException exception) {
        return asFileSystemException(directory, null, exception);
    }

    @Override
    public FileSystemException createChangeWorkingDirectoryException(String directory, SftpException exception) {
        return asFileSystemException(directory, null, exception);
    }

    @Override
    public FileSystemException createCreateDirectoryException(String directory, SftpException exception) {
        return asFileSystemException(directory, null, exception);
    }

    @Override
    public FileSystemException createDeleteException(String file, SftpException exception, boolean isDirectory) {
        return asFileSystemException(file, null, exception);
    }

    @Override
    public FileSystemException createNewInputStreamException(String file, SftpException exception) {
        return asFileSystemException(file, null, exception);
    }

    @Override
    public FileSystemException createNewOutputStreamException(String file, SftpException exception, Collection<? extends OpenOption> options) {
        return asFileSystemException(file, null, exception);
    }

    @Override
    public FileSystemException createCopyException(String file, String other, SftpException exception) {
        return asFileSystemException(file, other, exception);
    }

    @Override
    public FileSystemException createMoveException(String file, String other, SftpException exception) {
        return asFileSystemException(file, other, exception);
    }

    @Override
    public FileSystemException createSetOwnerException(String file, SftpException exception) {
        return asFileSystemException(file, null, exception);
    }

    @Override
    public FileSystemException createSetGroupException(String file, SftpException exception) {
        return asFileSystemException(file, null, exception);
    }

    @Override
    public FileSystemException createSetPermissionsException(String file, SftpException exception) {
        return asFileSystemException(file, null, exception);
    }

    @Override
    public FileSystemException createSetModificationTimeException(String file, SftpException exception) {
        return asFileSystemException(file, null, exception);
    }

    private FileSystemException asFileSystemException(String file, String other, SftpException e) {
        final FileSystemException exception;
        switch (e.id) {
        case ChannelSftp.SSH_FX_NO_SUCH_FILE:
            exception = new NoSuchFileException(file, other, e.getMessage());
            break;
        case ChannelSftp.SSH_FX_PERMISSION_DENIED:
            exception = new AccessDeniedException(file, other, e.getMessage());
            break;
        default:
            exception = new FileSystemException(file, other, e.getMessage());
            break;
        }
        exception.initCause(e);
        return exception;
    }
}
