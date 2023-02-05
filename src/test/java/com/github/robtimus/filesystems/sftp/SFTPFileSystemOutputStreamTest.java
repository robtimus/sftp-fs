/*
 * SFTPFileSystemOutputStreamTest.java
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

@SuppressWarnings("nls")
class SFTPFileSystemOutputStreamTest extends AbstractSFTPFileSystemTest {

    @Test
    void testWriteSingle() throws IOException {
        try (OutputStream output = provider().newOutputStream(createPath("/foo"))) {
            output.write('H');
            output.write('e');
            output.write('l');
            output.write('l');
            output.write('o');
        }
        Path file = getFile("/foo");
        assertEquals("Hello", getStringContents(file));
    }

    @Test
    void testWriteBulk() throws IOException {
        try (OutputStream output = provider().newOutputStream(createPath("/foo"))) {
            output.write("Hello".getBytes());
        }
        Path file = getFile("/foo");
        assertEquals("Hello", getStringContents(file));
    }
}
