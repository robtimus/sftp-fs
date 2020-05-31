/*
 * SFTPEnvironmentSetterTest.java
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import java.io.File;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import com.github.robtimus.filesystems.sftp.SFTPEnvironmentTest.TestHostKeyRepository;
import com.github.robtimus.filesystems.sftp.SFTPEnvironmentTest.TestIdentityRepository;
import com.github.robtimus.filesystems.sftp.SFTPEnvironmentTest.TestSocketFactory;
import com.jcraft.jsch.ProxyHTTP;

@SuppressWarnings({ "nls", "javadoc" })
public class SFTPEnvironmentSetterTest {

    private Method findMethod(String methodName, Class<?> propertyType) {
        for (Method method : SFTPEnvironment.class.getMethods()) {
            if (method.getName().equals(methodName)) {
                Class<?>[] parameterTypes = method.getParameterTypes();
                if (parameterTypes.length == 1) {
                    if (parameterTypes[0].isAssignableFrom(propertyType)) {
                        return method;
                    }
                    if (parameterTypes[0] == boolean.class && propertyType == Boolean.class) {
                        return method;
                    }
                    if (parameterTypes[0] == int.class && propertyType == Integer.class) {
                        return method;
                    }
                    if (parameterTypes[0] == long.class && propertyType == Long.class) {
                        return method;
                    }
                }
            }
        }
        throw new AssertionError("Could not find method " + methodName);
    }

    static Stream<Arguments> testSetter() {
        Arguments[] arguments = {
                arguments("withUsername", "username", UUID.randomUUID().toString()),
                arguments("withConnectTimeout", "connectTimeout", 1000),
                arguments("withProxy", "proxy", new ProxyHTTP("localhost")),
                arguments("withUserInfo", "userInfo", new SimpleUserInfo(UUID.randomUUID().toString().toCharArray())),
                arguments("withPassword", "password", UUID.randomUUID().toString().toCharArray()),
                arguments("withConfig", "config", System.getProperties()),
                arguments("withSocketFactory", "socketFactory", new TestSocketFactory()),
                arguments("withTimeout", "timeOut", 1000),
                arguments("withClientVersion", "clientVersion", "SSH-2"),
                arguments("withHostKeyAlias", "hostKeyAlias", "alias"),
                arguments("withServerAliveInterval", "serverAliveInterval", 500),
                arguments("withServerAliveCountMax", "serverAliveCountMax", 5),
                arguments("withIdentityRepository", "identityRepository", new TestIdentityRepository()),
                arguments("withIdentities", "identities", Collections.singletonList(IdentityTest.fromFiles())),
                arguments("withHostKeyRepository", "hostKeyRepository", new TestHostKeyRepository()),
                arguments("withKnownHosts", "knownHosts", new File("known_hosts")),
                arguments("withAgentForwarding", "agentForwarding", false),
                arguments("withFilenameEncoding", "filenameEncoding", "UTF-8"),
                arguments("withDefaultDirectory", "defaultDir", "/"),
                arguments("withClientConnectionCount", "clientConnectionCount", 5),
                arguments("withClientConnectionWaitTimeout", "clientConnectionWaitTimeout", 1000L),
                arguments("withFileSystemExceptionFactory", "fileSystemExceptionFactory", DefaultFileSystemExceptionFactory.INSTANCE),
                arguments("withActualTotalSpaceCalculation", "calculateActualTotalSpace", false),
        };
        return Arrays.stream(arguments);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    public void testSetter(String methodName, String propertyName, Object propertyValue) throws ReflectiveOperationException {
        Method setter = findMethod(methodName, propertyValue.getClass());

        SFTPEnvironment env = new SFTPEnvironment();

        assertEquals(Collections.emptyMap(), env);

        setter.invoke(env, propertyValue);

        assertEquals(Collections.singletonMap(propertyName, propertyValue), env);
    }
}
