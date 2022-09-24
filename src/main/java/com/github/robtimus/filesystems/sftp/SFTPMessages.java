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

    private static final ResourceBundle BUNDLE = getBundle();

    private SFTPMessages() {
        throw new IllegalStateException("cannot create instances of " + getClass().getName()); //$NON-NLS-1$
    }

    private static ResourceBundle getBundle() {
        final String baseName = "com.github.robtimus.filesystems.sftp.fs"; //$NON-NLS-1$
        try {
            return ResourceBundle.getBundle(baseName, UTF8Control.INSTANCE);
        } catch (@SuppressWarnings("unused") UnsupportedOperationException e) {
            // Java 9 or up; defaults to UTF-8
            return ResourceBundle.getBundle(baseName);
        }
    }

    private static synchronized String getMessage(String key) {
        return BUNDLE.getString(key);
    }

    private static String getMessage(String key, Object... args) {
        String format = getMessage(key);
        return String.format(format, args);
    }

    static String copyOfSymbolicLinksAcrossFileSystemsNotSupported() {
        return getMessage("copyOfSymbolicLinksAcrossFileSystemsNotSupported"); //$NON-NLS-1$
    }

    static String clientConnectionWaitTimeoutExpired() {
        return getMessage("clientConnectionWaitTimeoutExpired"); //$NON-NLS-1$
    }

    static String createdInputStream(String path) {
        return getMessage("log.createdInputStream", path); //$NON-NLS-1$
    }

    static String closedInputStream(String path) {
        return getMessage("log.closedInputStream", path); //$NON-NLS-1$
    }

    static String createdOutputStream(String path) {
        return getMessage("log.createdOutputStream", path); //$NON-NLS-1$
    }

    static String closedOutputStream(String path) {
        return getMessage("log.closedOutputStream", path); //$NON-NLS-1$
    }
}
