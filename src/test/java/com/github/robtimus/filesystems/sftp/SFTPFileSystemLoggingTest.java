/*
 * SFTPFileSystemLoggingTest.java
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.varia.NullAppender;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("nls")
class SFTPFileSystemLoggingTest extends AbstractSFTPFileSystemTest {

    private static Logger logger;
    private static Level originalLevel;
    private static List<Appender> originalAppenders;
    private static List<Appender> originalRootAppenders;

    private Appender appender;

    @BeforeAll
    static void setupLogging() {
        logger = LogManager.getLogger(SSHChannelPool.class.getPackage().getName());
        originalLevel = logger.getLevel();
        logger.setLevel(Level.TRACE);
        originalAppenders = getAllAppenders(logger);
        logger.removeAllAppenders();

        Logger root = LogManager.getRootLogger();
        originalRootAppenders = getAllAppenders(root);
        root.removeAllAppenders();
    }

    private static List<Appender> getAllAppenders(Logger l) {
        List<Appender> appenders = new ArrayList<>();
        for (@SuppressWarnings("unchecked") Enumeration<Appender> e = l.getAllAppenders(); e.hasMoreElements(); ) {
            appenders.add(e.nextElement());
        }
        return appenders;
    }

    @AfterAll
    static void clearLogging() {
        logger.setLevel(originalLevel);
        for (Appender appender : originalAppenders) {
            logger.addAppender(appender);
        }

        Logger root = LogManager.getRootLogger();
        for (Appender appender : originalRootAppenders) {
            root.addAppender(appender);
        }
    }

    @BeforeEach
    void setupAppender() {
        appender = spy(new NullAppender());
        logger.addAppender(appender);
    }

    @AfterEach
    void clearAppender() {
        logger.removeAppender(appender);
    }

    @Test
    void testLogging() throws IOException {
        URI uri = getURI();
        try (SFTPFileSystem fs = newFileSystem(uri, createEnv())) {
            SFTPFileSystemProvider.keepAlive(fs);
        }

        ArgumentCaptor<LoggingEvent> captor = ArgumentCaptor.forClass(LoggingEvent.class);
        verify(appender, atLeast(1)).doAppend(captor.capture());
        List<String> debugMessages = new ArrayList<>();
        Set<String> names = new HashSet<>();
        Set<Level> levels = new HashSet<>();
        for (LoggingEvent event : captor.getAllValues()) {
            names.add(event.getLoggerName());
            levels.add(event.getLevel());
            if (Level.DEBUG.equals(event.getLevel())) {
                String message = assertInstanceOf(String.class, event.getMessage());
                debugMessages.add(message);
            }
        }

        assertEquals(Collections.singleton(SSHChannelPool.class.getName()), names);
        assertEquals(Collections.singleton(Level.DEBUG), levels);

        // Don't test all messages, that's all handled by the pool
        // Just test the prefixes
        String hostname = uri.getHost();
        int port = uri.getPort();
        String prefix = port == -1 ? hostname : hostname + ":" + port;
        assertThat(debugMessages, everyItem(startsWith(prefix + " - ")));

        assertThat(debugMessages, hasItem(matchesRegex(prefix + " - channel-\\d+ created")));
    }

    private SFTPFileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        return (SFTPFileSystem) FileSystems.newFileSystem(uri, env);
    }
}
