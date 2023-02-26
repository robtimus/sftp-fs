/*
 * CopyOptionsTest.java
 * Copyright 2022 Rob Spoor
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.CopyOption;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import com.github.robtimus.filesystems.Messages;

class CopyOptionsTest {

    @Test
    void testToOpenOptions() {
        CopyOptions options = CopyOptions.forCopy(StandardCopyOption.REPLACE_EXISTING, LinkOption.NOFOLLOW_LINKS);
        Collection<OpenOption> openOptions = options.toOpenOptions(StandardOpenOption.READ);
        assertEquals(Arrays.asList(LinkOption.NOFOLLOW_LINKS, StandardOpenOption.READ), openOptions);
    }

    @Nested
    class ForCopy {

        @Test
        void testWithNoOptions() {
            CopyOptions options = CopyOptions.forCopy();
            assertFalse(options.replaceExisting);
        }

        @Test
        void testWithReplaceExisting() {
            CopyOptions options = CopyOptions.forCopy(StandardCopyOption.REPLACE_EXISTING);

            assertTrue(options.replaceExisting);
        }

        @Test
        void testWithNoFollowLinks() {
            CopyOptions options = CopyOptions.forCopy(LinkOption.NOFOLLOW_LINKS);

            assertFalse(options.replaceExisting);
        }

        @Test
        void testWithReplaceExistingAndNoFollowLinks() {
            CopyOptions options = CopyOptions.forCopy(StandardCopyOption.REPLACE_EXISTING, LinkOption.NOFOLLOW_LINKS);

            assertTrue(options.replaceExisting);
        }

        @Test
        void testWithCopyAttributes() {
            testWithInvalid(StandardCopyOption.COPY_ATTRIBUTES);
        }

        @Test
        void testWithAtomicMove() {
            testWithInvalid(StandardCopyOption.ATOMIC_MOVE);
        }

        private void testWithInvalid(CopyOption option) {
            UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class, () -> CopyOptions.forCopy(option));
            assertChainEquals(Messages.fileSystemProvider().unsupportedCopyOption(option), exception);
        }
    }

    @Nested
    class ForMove {

        @Nested
        class SameFileSystem {

            @Test
            void testWithNoOptions() {
                CopyOptions options = CopyOptions.forMove(true);
                assertFalse(options.replaceExisting);
            }

            @Test
            void testWithReplaceExisting() {
                CopyOptions options = CopyOptions.forMove(true, StandardCopyOption.REPLACE_EXISTING);

                assertTrue(options.replaceExisting);
            }

            @Test
            void testWithAtomicMove() {
                CopyOptions options = CopyOptions.forMove(true, StandardCopyOption.ATOMIC_MOVE);

                assertFalse(options.replaceExisting);
            }

            @Test
            void testWithReplaceExistingAndAtomicMove() {
                CopyOptions options = CopyOptions.forMove(true, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

                assertTrue(options.replaceExisting);
            }

            @Test
            void testWithNoFollowLinks() {
                CopyOptions options = CopyOptions.forMove(true, LinkOption.NOFOLLOW_LINKS);

                assertFalse(options.replaceExisting);
            }

            @Test
            void testWithReplaceExistingAndNoFollowLinks() {
                CopyOptions options = CopyOptions.forMove(true, StandardCopyOption.REPLACE_EXISTING, LinkOption.NOFOLLOW_LINKS);

                assertTrue(options.replaceExisting);
            }

            @Test
            void testWithAtomicMoveAndNoFollowLinks() {
                CopyOptions options = CopyOptions.forMove(true, StandardCopyOption.ATOMIC_MOVE, LinkOption.NOFOLLOW_LINKS);

                assertFalse(options.replaceExisting);
            }

            @Test
            void testWithReplaceExistingAndAtomicMoveAndNoFollowLinks() {
                CopyOptions options = CopyOptions.forMove(true, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE,
                        LinkOption.NOFOLLOW_LINKS);

                assertTrue(options.replaceExisting);
            }

            @Test
            void testWithCopyAttributes() {
                testWithInvalid(true, StandardCopyOption.COPY_ATTRIBUTES);
            }
        }

        @Nested
        class DifferentFileSystem {

            @Test
            void testWithNoOptions() {
                CopyOptions options = CopyOptions.forMove(false);

                assertFalse(options.replaceExisting);
            }

            @Test
            void testWithReplaceExisting() {
                CopyOptions options = CopyOptions.forMove(false, StandardCopyOption.REPLACE_EXISTING);

                assertTrue(options.replaceExisting);
            }

            @Test
            void testWithNoFollowLinks() {
                CopyOptions options = CopyOptions.forMove(false, LinkOption.NOFOLLOW_LINKS);

                assertFalse(options.replaceExisting);
            }

            @Test
            void testWithReplaceExistingAndNoFollowLinks() {
                CopyOptions options = CopyOptions.forMove(false, StandardCopyOption.REPLACE_EXISTING, LinkOption.NOFOLLOW_LINKS);

                assertTrue(options.replaceExisting);
            }

            @Test
            void testWithCopyAttributes() {
                testWithInvalid(false, StandardCopyOption.COPY_ATTRIBUTES);
            }

            @Test
            void testWithAtomicMove() {
                testWithInvalid(false, StandardCopyOption.ATOMIC_MOVE);
            }
        }

        private void testWithInvalid(boolean sameFileSystem, CopyOption option) {
            UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                    () -> CopyOptions.forMove(sameFileSystem, option));
            assertChainEquals(Messages.fileSystemProvider().unsupportedCopyOption(option), exception);
        }
    }
}
