/*
 * SFTPFileSystemDirectoryStreamTest.java
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

import static com.github.robtimus.junit.support.ThrowableAssertions.assertChainEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.matchesRegex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import com.github.robtimus.filesystems.Messages;

@SuppressWarnings("nls")
class SFTPFileSystemDirectoryStreamTest extends AbstractSFTPFileSystemTest {

    @Test
    void testIterator() throws IOException {
        final int count = 100;

        List<Matcher<? super String>> matchers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            matchers.add(equalTo("file" + i));
            addFile("/foo/file" + i);
        }

        List<String> names = new ArrayList<>();
        try (DirectoryStream<Path> stream = provider().newDirectoryStream(createPath("/foo"), entry -> true)) {
            for (Iterator<Path> iterator = stream.iterator(); iterator.hasNext(); ) {
                names.add(iterator.next().getFileName().toString());
            }
        }
        assertThat(names, containsInAnyOrder(matchers));
    }

    @Test
    void testFilteredIterator() throws IOException {
        final int count = 100;

        List<Matcher<? super String>> matchers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            if (i % 2 == 1) {
                matchers.add(equalTo("file" + i));
            }
            addFile("/foo/file" + i);
        }

        List<String> names = new ArrayList<>();
        Filter<Path> filter = new PatternFilter("file\\d*[13579]");
        try (DirectoryStream<Path> stream = provider().newDirectoryStream(createPath("/foo"), filter)) {
            for (Iterator<Path> iterator = stream.iterator(); iterator.hasNext(); ) {
                names.add(iterator.next().getFileName().toString());
            }
        }
        assertThat(names, containsInAnyOrder(matchers));
    }

    @Test
    void testCloseWhileIterating() throws IOException {
        final int count = 100;

        // there is no guaranteed order, just a count
        for (int i = 0; i < count; i++) {
            addFile("/foo/file" + i);
        }
        Matcher<String> matcher = matchesRegex("file\\d+");
        int expectedCount = count / 2;

        List<String> names = new ArrayList<>();
        try (DirectoryStream<Path> stream = provider().newDirectoryStream(createPath("/foo"), entry -> true)) {

            int index = 0;
            for (Iterator<Path> iterator = stream.iterator(); iterator.hasNext(); ) {
                if (++index == count / 2) {
                    stream.close();
                }
                names.add(iterator.next().getFileName().toString());
            }
        }
        assertEquals(expectedCount, names.size());
        assertThat(names, everyItem(matcher));
    }

    @Test
    void testIteratorAfterClose() throws IOException {
        try (DirectoryStream<Path> stream = provider().newDirectoryStream(createPath("/"), entry -> true)) {
            stream.close();
            IllegalStateException exception = assertThrows(IllegalStateException.class, stream::iterator);
            assertChainEquals(Messages.directoryStream().closed(), exception);
        }
    }

    @Test
    void testIteratorAfterIterator() throws IOException {
        try (DirectoryStream<Path> stream = provider().newDirectoryStream(createPath("/"), entry -> true)) {
            stream.iterator();
            IllegalStateException exception = assertThrows(IllegalStateException.class, stream::iterator);
            assertChainEquals(Messages.directoryStream().iteratorAlreadyReturned(), exception);
        }
    }

    @Test
    void testDeleteWhileIterating() throws IOException {
        final int count = 100;

        List<Matcher<? super String>> matchers = new ArrayList<>();
        addDirectory("/foo");
        for (int i = 0; i < count; i++) {
            matchers.add(equalTo("file" + i));
            addFile("/foo/file" + i);
        }

        List<String> names = new ArrayList<>();
        try (DirectoryStream<Path> stream = provider().newDirectoryStream(createPath("/foo"), entry -> true)) {

            int index = 0;
            for (Iterator<Path> iterator = stream.iterator(); iterator.hasNext(); ) {
                if (++index < count / 2) {
                    delete("/foo");
                }
                names.add(iterator.next().getFileName().toString());
            }
        }
        assertThat(names, containsInAnyOrder(matchers));
    }

    @Test
    void testDeleteChildrenWhileIterating() throws IOException {
        final int count = 100;

        List<Matcher<? super String>> matchers = new ArrayList<>();
        addDirectory("/foo");
        for (int i = 0; i < count; i++) {
            matchers.add(equalTo("file" + i));
            addFile("/foo/file" + i);
        }

        List<String> names = new ArrayList<>();
        try (DirectoryStream<Path> stream = provider().newDirectoryStream(createPath("/foo"), entry -> true)) {

            int index = 0;
            for (Iterator<Path> iterator = stream.iterator(); iterator.hasNext(); ) {
                if (++index < count / 2) {
                    for (int i = 0; i < count; i++) {
                        delete("/foo/file" + i);
                    }
                    assertEquals(0, getChildCount("/foo"));
                }
                names.add(iterator.next().getFileName().toString());
            }
        }
        assertThat(names, containsInAnyOrder(matchers));
    }

    @Test
    void testDeleteBeforeIterator() throws IOException {
        final int count = 100;

        List<Matcher<? super String>> matchers = new ArrayList<>();
        addDirectory("/foo");
        for (int i = 0; i < count; i++) {
            // the entries are collected before the iteration starts
            matchers.add(equalTo("file" + i));
            addFile("/foo/file" + i);
        }

        List<String> names = new ArrayList<>();
        try (DirectoryStream<Path> stream = provider().newDirectoryStream(createPath("/foo"), entry -> true)) {

            delete("/foo");
            for (Iterator<Path> iterator = stream.iterator(); iterator.hasNext(); ) {
                names.add(iterator.next().getFileName().toString());
            }
        }
        assertThat(names, containsInAnyOrder(matchers));
    }

    @Test
    void testThrowWhileIterating() throws IOException {
        addFile("/foo");

        Filter<Path> filter = entry -> {
            throw new IOException();
        };
        try (DirectoryStream<Path> stream = provider().newDirectoryStream(createPath("/"), filter)) {
            Iterator<Path> iterator = stream.iterator();
            // hasNext already uses the filter, and therefore already causes the exception to be thrown
            DirectoryIteratorException exception = assertThrows(DirectoryIteratorException.class, iterator::hasNext);
            assertThat(exception.getCause(), instanceOf(IOException.class));
        }
    }

    private static final class PatternFilter implements Filter<Path> {

        private final Pattern pattern;

        private PatternFilter(String regex) {
            pattern = Pattern.compile(regex);
        }

        @Override
        public boolean accept(Path entry) {
            return pattern.matcher(entry.getFileName().toString()).matches();
        }
    }
}
