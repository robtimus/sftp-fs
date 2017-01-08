/*
 * SFTPMessages.java
 * Copyright 2017 Rob Spoor
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

import java.util.ResourceBundle;
import com.github.robtimus.filesystems.UTF8Control;

/**
 * A utility class for providing translated messages and exceptions.
 *
 * @author Rob Spoor
 */
final class SFTPMessages {

    private static final String BUNDLE_NAME = "com.github.robtimus.filesystems.sftp.fs"; //$NON-NLS-1$
    private static final ResourceBundle BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME, UTF8Control.INSTANCE);

    private SFTPMessages() {
        throw new Error("cannot create instances of " + getClass().getName()); //$NON-NLS-1$
    }

    private static synchronized String getMessage(String key) {
        return BUNDLE.getString(key);
    }

    public static String copyOfSymbolicLinksAcrossFileSystemsNotSupported() {
        return getMessage("copyOfSymbolicLinksAcrossFileSystemsNotSupported"); //$NON-NLS-1$
    }
}
