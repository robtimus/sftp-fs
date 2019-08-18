/*
 * SFTPLogger.java
 * Copyright 2019 Rob Spoor
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
import java.util.ResourceBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.robtimus.filesystems.UTF8Control;

/**
 * A utility class to perform logging.
 *
 * @author Rob Spoor
 */
final class SFTPLogger {

    private static final String BUNDLE_NAME = "com.github.robtimus.filesystems.sftp.fs"; //$NON-NLS-1$
    private static final ResourceBundle BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME, UTF8Control.INSTANCE);

    private SFTPLogger() {
        throw new Error("cannot create instances of " + getClass().getName()); //$NON-NLS-1$
    }

    public static Logger createLogger(Class<?> clazz) {
        try {
            return LoggerFactory.getLogger(clazz);
        } catch (@SuppressWarnings("unused") NoClassDefFoundError e) {
            return null;
        }
    }

    private static synchronized String getMessage(String key) {
        return BUNDLE.getString(key);
    }

    public static void creatingPool(Logger logger, String hostname, int port, int poolSize, long poolWaitTimeout) {
        if (logger != null && logger.isDebugEnabled()) {
            if (port == -1) {
                logger.debug(String.format(getMessage("log.creatingPoolWithoutPort"), hostname, poolSize, poolWaitTimeout)); //$NON-NLS-1$
            } else {
                logger.debug(String.format(getMessage("log.creatingPoolWithPort"), hostname, port, poolSize, poolWaitTimeout)); //$NON-NLS-1$
            }
        }
    }

    public static void createdPool(Logger logger, String hostname, int port, int poolSize) {
        if (logger != null && logger.isDebugEnabled()) {
            if (port == -1) {
                logger.debug(String.format(getMessage("log.createdPoolWithoutPort"), hostname, poolSize)); //$NON-NLS-1$
            } else {
                logger.debug(String.format(getMessage("log.createdPoolWithPort"), hostname, port, poolSize)); //$NON-NLS-1$
            }
        }
    }

    public static void failedToCreatePool(Logger logger, IOException e) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(getMessage("log.failedToCreatePool"), e); //$NON-NLS-1$
        }
    }

    public static void createdChannel(Logger logger, String channelId, boolean pooled) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(String.format(getMessage("log.createdChannel"), channelId, pooled)); //$NON-NLS-1$
        }
    }

    public static void tookChannel(Logger logger, String channelId, int poolSize) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(String.format(getMessage("log.tookChannel"), channelId, poolSize)); //$NON-NLS-1$
        }
    }

    public static void channelNotConnected(Logger logger, String channelId) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(String.format(getMessage("log.channelNotConnected"), channelId)); //$NON-NLS-1$
        }
    }

    public static void returnedChannel(Logger logger, String channelId, int poolSize) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(String.format(getMessage("log.returnedChannel"), channelId, poolSize)); //$NON-NLS-1$
        }
    }

    public static void returnedBrokenChannel(Logger logger, String channelId, int poolSize) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(String.format(getMessage("log.returnedBrokenChannel"), channelId, poolSize)); //$NON-NLS-1$
        }
    }

    public static void drainedPoolForKeepAlive(Logger logger) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(getMessage("log.drainedPoolForKeepAlive")); //$NON-NLS-1$
        }
    }

    public static void drainedPoolForClose(Logger logger) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(getMessage("log.drainedPoolForClose")); //$NON-NLS-1$
        }
    }

    public static void increasedRefCount(Logger logger, String channelId, int refCount) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(String.format(getMessage("log.increasedRefCount"), channelId, refCount)); //$NON-NLS-1$
        }
    }

    public static void decreasedRefCount(Logger logger, String channelId, int refCount) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(String.format(getMessage("log.decreasedRefCount"), channelId, refCount)); //$NON-NLS-1$
        }
    }

    public static void disconnectedChannel(Logger logger, String channelId) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(String.format(getMessage("log.disconnectedChannel"), channelId)); //$NON-NLS-1$
        }
    }

    public static void createdInputStream(Logger logger, String channelId, String path) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(String.format(getMessage("log.createdInputStream"), channelId, path)); //$NON-NLS-1$
        }
    }

    public static void closedInputStream(Logger logger, String channelId, String path) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(String.format(getMessage("log.closedInputStream"), channelId, path)); //$NON-NLS-1$
        }
    }

    public static void createdOutputStream(Logger logger, String channelId, String path) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(String.format(getMessage("log.createdOutputStream"), channelId, path)); //$NON-NLS-1$
        }
    }

    public static void closedOutputStream(Logger logger, String channelId, String path) {
        if (logger != null && logger.isDebugEnabled()) {
            logger.debug(String.format(getMessage("log.closedOutputStream"), channelId, path)); //$NON-NLS-1$
        }
    }
}
