/*
 * CopyOptions.java
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

import java.nio.file.CopyOption;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import com.github.robtimus.filesystems.Messages;

/**
 * A representation of possible copy options.
 *
 * @author Rob Spoor
 */
final class CopyOptions {

    public final boolean replaceExisting;

    public final Collection<? extends CopyOption> options;

    private CopyOptions(boolean replaceExisting,
            Collection<? extends CopyOption> options) {

        this.replaceExisting = replaceExisting;

        this.options = options;
    }

    public Collection<OpenOption> toOpenOptions(OpenOption... additional) {
        List<OpenOption> openOptions = new ArrayList<>(options.size() + additional.length);
        for (CopyOption option : options) {
            if (option instanceof OpenOption) {
                openOptions.add((OpenOption) option);
            }
        }
        Collections.addAll(openOptions, additional);
        return openOptions;
    }

    static CopyOptions forCopy(CopyOption... options) {

        boolean replaceExisting = false;

        for (CopyOption option : options) {
            if (option == StandardCopyOption.REPLACE_EXISTING) {
                replaceExisting = true;
            } else if (!isIgnoredCopyOption(option)) {
                throw Messages.fileSystemProvider().unsupportedCopyOption(option);
            }
        }

        return new CopyOptions(replaceExisting, Arrays.asList(options));
    }

    static CopyOptions forMove(CopyOption... options) {
        boolean replaceExisting = false;

        for (CopyOption option : options) {
            if (option == StandardCopyOption.REPLACE_EXISTING) {
                replaceExisting = true;
            } else if (!isIgnoredCopyOption(option)) {
                throw Messages.fileSystemProvider().unsupportedCopyOption(option);
            }
        }

        return new CopyOptions(replaceExisting, Arrays.asList(options));
    }

    private static boolean isIgnoredCopyOption(CopyOption option) {
        return option == StandardCopyOption.ATOMIC_MOVE
                || option == LinkOption.NOFOLLOW_LINKS;
    }
}
