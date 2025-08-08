/*
 * OpenOptions.java
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

import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import com.github.robtimus.filesystems.Messages;

/**
 * A representation of possible open options.
 *
 * @author Rob Spoor
 */
final class OpenOptions {

    final boolean read;
    final boolean write;
    final boolean append;
    final boolean create;
    final boolean createNew;
    final boolean deleteOnClose;

    final Collection<? extends OpenOption> options;

    private OpenOptions(boolean read, boolean write, boolean append, boolean create, boolean createNew, boolean deleteOnClose,
            Collection<? extends OpenOption> options) {

        this.read = read;
        this.write = write;
        this.append = append;
        this.create = create;
        this.createNew = createNew;
        this.deleteOnClose = deleteOnClose;

        this.options = options;
    }

    static OpenOptions forNewInputStream(OpenOption... options) {
        return forNewInputStream(Arrays.asList(options));
    }

    static OpenOptions forNewInputStream(Collection<? extends OpenOption> options) {
        if (options.isEmpty()) {
            return new OpenOptions(true, false, false, false, false, false, Collections.emptySet());
        }

        boolean deleteOnClose = false;

        for (OpenOption option : options) {
            if (option == StandardOpenOption.DELETE_ON_CLOSE) {
                deleteOnClose = true;
            } else if (option != StandardOpenOption.READ && !isIgnoredOpenOption(option)) {
                throw Messages.fileSystemProvider().unsupportedOpenOption(option);
            }
        }

        return new OpenOptions(true, false, false, false, false, deleteOnClose, options);
    }

    static OpenOptions forNewOutputStream(OpenOption... options) {
        return forNewOutputStream(Arrays.asList(options));
    }

    static OpenOptions forNewOutputStream(Collection<? extends OpenOption> options) {
        if (options.isEmpty()) {
            // CREATE, TRUNCATE_EXISTING and WRITE, i.e. create, not createNew, and not append
            return new OpenOptions(false, true, false, true, false, false, Collections.emptySet());
        }

        boolean append = false;
        boolean truncateExisting = false;
        boolean create = false;
        boolean createNew = false;
        boolean deleteOnClose = false;

        for (OpenOption option : options) {
            if (option == StandardOpenOption.APPEND) {
                append = true;
            } else if (option == StandardOpenOption.TRUNCATE_EXISTING) {
                truncateExisting = true;
            } else if (option == StandardOpenOption.CREATE) {
                create = true;
            } else if (option == StandardOpenOption.CREATE_NEW) {
                createNew = true;
            } else if (option == StandardOpenOption.DELETE_ON_CLOSE) {
                deleteOnClose = true;
            } else if (option != StandardOpenOption.WRITE && !isIgnoredOpenOption(option)) {
                throw Messages.fileSystemProvider().unsupportedOpenOption(option);
            }
        }

        // append and truncateExisting contradict each other
        if (append && truncateExisting) {
            throw Messages.fileSystemProvider().illegalOpenOptionCombination(options);
        }

        return new OpenOptions(false, true, append, create, createNew, deleteOnClose, options);
    }

    static OpenOptions forNewByteChannel(Set<? extends OpenOption> options) {

        boolean read = false;
        boolean write = false;
        boolean append = false;
        boolean truncateExisting = false;
        boolean create = false;
        boolean createNew = false;
        boolean deleteOnClose = false;

        for (OpenOption option : options) {
            if (option == StandardOpenOption.READ) {
                read = true;
            } else if (option == StandardOpenOption.WRITE) {
                write = true;
            } else if (option == StandardOpenOption.APPEND) {
                append = true;
            } else if (option == StandardOpenOption.TRUNCATE_EXISTING) {
                truncateExisting = true;
            } else if (option == StandardOpenOption.CREATE) {
                create = true;
            } else if (option == StandardOpenOption.CREATE_NEW) {
                createNew = true;
            } else if (option == StandardOpenOption.DELETE_ON_CLOSE) {
                deleteOnClose = true;
            } else if (!isIgnoredOpenOption(option)) {
                throw Messages.fileSystemProvider().unsupportedOpenOption(option);
            }
        }

        // as per Files.newByteChannel, if none of these options is given, default to read
        if (!read && !write && !append) {
            read = true;
        }

        // APPEND may not be used with READ or TRUNCATE_EXISTING
        if (append && (read || truncateExisting)) {
            throw Messages.fileSystemProvider().illegalOpenOptionCombination(options);
        }

        if (append) {
            // !read, open in write-only mode
            write = true;
        }

        // although Files.newByteChannel supports READ + WRITE, SFTPFileSystem does not
        if (read && write) {
            throw Messages.fileSystemProvider().illegalOpenOptionCombination(options);
        }

        return new OpenOptions(read, write, append, create, createNew, deleteOnClose, options);
    }

    private static boolean isIgnoredOpenOption(OpenOption option) {
        return option == StandardOpenOption.SPARSE
                || option == StandardOpenOption.SYNC
                || option == StandardOpenOption.DSYNC
                || option == LinkOption.NOFOLLOW_LINKS;
    }
}
