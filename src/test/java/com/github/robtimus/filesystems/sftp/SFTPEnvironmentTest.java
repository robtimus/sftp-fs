/*
 * SFTPEnvironmentTest.java
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

import static org.junit.Assert.assertEquals;
import java.util.Collections;
import java.util.Properties;
import org.junit.Test;

@SuppressWarnings({ "nls", "javadoc" })
public class SFTPEnvironmentTest {

    @Test
    public void testWithConfig() {
        SFTPEnvironment env = new SFTPEnvironment();

        assertEquals(Collections.emptyMap(), env);

        String key1 = "key1";
        String value1 = "value1";
        String key2 = "key2";
        String value2 = "value2";

        env.withConfig(key1, value1);
        env.withConfig(key2, value2);

        Properties properties = new Properties();
        properties.setProperty(key1, value1);
        properties.setProperty(key2, value2);

        assertEquals(Collections.singletonMap("config", properties), env);
    }
}
