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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import com.github.robtimus.junit.support.extension.testlogger.Reload4jLoggerContext;
import com.github.robtimus.junit.support.extension.testlogger.TestLogger;

@SuppressWarnings("nls")
class SFTPFileSystemLoggingTest extends AbstractSFTPFileSystemTest {

    @Test
    void testLogging(@TestLogger.ForClass(SSHChannelPool.class) Reload4jLoggerContext logger) throws IOException {
        Appender appender = mock(Appender.class);
        logger.setLevel(Level.TRACE)
                .addAppender(appender)
                .useParentAppenders(false);

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
