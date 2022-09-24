/*
 * SFTPPoolConfigTest.java
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import com.github.robtimus.filesystems.sftp.SFTPPoolConfig.Builder;

@SuppressWarnings("nls")
class SFTPPoolConfigTest {

    @Nested
    @DisplayName("Builder")
    class BuilderTest {

        @Nested
        @DisplayName("maxWaitTime")
        class MaxWaitTime {

            @Test
            @DisplayName("default value")
            void testDefaultValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .build();

                assertEquals(Optional.empty(), config.maxWaitTime());
            }

            @Test
            @DisplayName("null value")
            void testNullValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .withMaxWaitTime(null)
                        .build();

                assertEquals(Optional.empty(), config.maxWaitTime());
            }

            @Test
            @DisplayName("negative value")
            void testNegativeValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .withMaxWaitTime(Duration.ofNanos(-1))
                        .build();

                assertEquals(Optional.empty(), config.maxWaitTime());
            }

            @Test
            @DisplayName("0 value")
            void testZeroValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .withMaxWaitTime(Duration.ZERO)
                        .build();

                assertEquals(Optional.of(Duration.ZERO), config.maxWaitTime());
            }

            @Test
            @DisplayName("positive value")
            void testPositiveValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .withMaxWaitTime(Duration.ofNanos(1))
                        .build();

                assertEquals(Optional.of(Duration.ofNanos(1)), config.maxWaitTime());
            }
        }

        @Nested
        @DisplayName("maxIdleTime")
        class MaxIdleTime {

            @Test
            @DisplayName("default value")
            void testDefaultValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .build();

                assertEquals(Optional.empty(), config.maxIdleTime());
            }

            @Test
            @DisplayName("null value")
            void testNullValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .withMaxIdleTime(null)
                        .build();

                assertEquals(Optional.empty(), config.maxIdleTime());
            }

            @Test
            @DisplayName("negative value")
            void testNegativeValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .withMaxIdleTime(Duration.ofNanos(-1))
                        .build();

                assertEquals(Optional.of(Duration.ofNanos(-1)), config.maxIdleTime());
            }

            @Test
            @DisplayName("0 value")
            void testZeroValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .withMaxIdleTime(Duration.ZERO)
                        .build();

                assertEquals(Optional.of(Duration.ZERO), config.maxIdleTime());
            }

            @Test
            @DisplayName("positive value")
            void testPositiveValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .withMaxIdleTime(Duration.ofNanos(1))
                        .build();

                assertEquals(Optional.of(Duration.ofNanos(1)), config.maxIdleTime());
            }
        }

        @Nested
        @DisplayName("initialSize")
        class InitialSize {

            @Test
            @DisplayName("default value")
            void testDefaultValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .build();

                assertEquals(1, config.initialSize());
                assertEquals(5, config.maxSize());
            }

            @Test
            @DisplayName("negative value")
            void testNegativeValue() {
                Builder builder = SFTPPoolConfig.custom();

                assertThrows(IllegalArgumentException.class, () -> builder.withInitialSize(-1));

                SFTPPoolConfig config = builder.build();

                assertEquals(1, config.initialSize());
                assertEquals(5, config.maxSize());
            }

            @Test
            @DisplayName("0 value")
            void testZeroValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .withInitialSize(0)
                        .build();

                assertEquals(0, config.initialSize());
                assertEquals(5, config.maxSize());
            }

            @Test
            @DisplayName("positive value")
            void testPositiveValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .withInitialSize(1)
                        .build();

                assertEquals(1, config.initialSize());
                assertEquals(5, config.maxSize());
            }

            @Test
            @DisplayName("value larger than maxSize")
            void testValueLargerThanMaxSize() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .withInitialSize(10)
                        .build();

                assertEquals(10, config.initialSize());
                assertEquals(10, config.maxSize());
            }
        }

        @Nested
        @DisplayName("maxSize")
        class MaxSize {

            @Test
            @DisplayName("default value")
            void testDefaultValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .build();

                assertEquals(5, config.maxSize());
                assertEquals(1, config.initialSize());
            }

            @Test
            @DisplayName("negative value")
            void testNegativeValue() {
                Builder builder = SFTPPoolConfig.custom();

                assertThrows(IllegalArgumentException.class, () -> builder.withMaxSize(-1));

                SFTPPoolConfig config = builder.build();

                assertEquals(5, config.maxSize());
                assertEquals(1, config.initialSize());
            }

            @Test
            @DisplayName("0 value")
            void testZeroValue() {
                Builder builder = SFTPPoolConfig.custom();

                assertThrows(IllegalArgumentException.class, () -> builder.withMaxSize(0));

                SFTPPoolConfig config = builder.build();

                assertEquals(5, config.maxSize());
                assertEquals(1, config.initialSize());
            }

            @Test
            @DisplayName("positive value")
            void testPositiveValue() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .withMaxSize(1)
                        .build();

                assertEquals(1, config.maxSize());
                assertEquals(1, config.initialSize());
            }

            @Test
            @DisplayName("value smaller than initialSize")
            void testValueSmallerThanInitialSize() {
                SFTPPoolConfig config = SFTPPoolConfig.custom()
                        .withInitialSize(20)
                        .withMaxSize(10)
                        .build();

                assertEquals(10, config.initialSize());
                assertEquals(10, config.maxSize());
            }
        }
    }

    @Nested
    @DisplayName("toString")
    class ToString {

        @Test
        @DisplayName("without maxWaitTime or maxIdleTime")
        void testWithoutMaxIdleTime() {
            SFTPPoolConfig config = SFTPPoolConfig.custom()
                    .build();

            assertEquals("SFTPPoolConfig[maxWaitTime=null,maxIdleTime=null,initialSize=1,maxSize=5]", config.toString());
        }

        @Test
        @DisplayName("with maxWaitTime and maxIdleTime")
        void testWithMaxIdleTime() {
            SFTPPoolConfig config = SFTPPoolConfig.custom()
                    .withMaxWaitTime(Duration.ZERO)
                    .withMaxIdleTime(Duration.ofSeconds(5))
                    .build();

            assertEquals("SFTPPoolConfig[maxWaitTime=PT0S,maxIdleTime=PT5S,initialSize=1,maxSize=5]", config.toString());
        }
    }
}
