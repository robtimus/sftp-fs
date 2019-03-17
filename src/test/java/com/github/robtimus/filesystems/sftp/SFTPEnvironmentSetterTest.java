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

import static org.junit.Assert.assertEquals;
import java.io.File;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import com.github.robtimus.filesystems.sftp.SFTPEnvironmentTest.TestHostKeyRepository;
import com.github.robtimus.filesystems.sftp.SFTPEnvironmentTest.TestIdentityRepository;
import com.github.robtimus.filesystems.sftp.SFTPEnvironmentTest.TestSocketFactory;
import com.jcraft.jsch.ProxyHTTP;

@RunWith(Parameterized.class)
@SuppressWarnings({ "nls", "javadoc" })
public class SFTPEnvironmentSetterTest {

    private final Method setter;
    private final String propertyName;
    private final Object propertyValue;

    public SFTPEnvironmentSetterTest(String methodName, String propertyName, Object propertyValue) {
        this.setter = findMethod(methodName, propertyValue.getClass());
        this.propertyName = propertyName;
        this.propertyValue = propertyValue;
    }

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
                }
            }
        }
        throw new AssertionError("Could not find method " + methodName);
    }

    @Parameters(name = "{0}")
    public static List<Object[]> getParameters() {
        Object[][] parameters = {
                { "withUsername", "username", UUID.randomUUID().toString(), },
                { "withConnectTimeout", "connectTimeout", 1000, },
                { "withProxy", "proxy", new ProxyHTTP("localhost"), },
                { "withUserInfo", "userInfo", new SimpleUserInfo(UUID.randomUUID().toString().toCharArray()), },
                { "withPassword", "password", UUID.randomUUID().toString().toCharArray(), },
                { "withConfig", "config", System.getProperties(), },
                { "withSocketFactory", "socketFactory", new TestSocketFactory(), },
                { "withTimeout", "timeOut", 1000, },
                { "withClientVersion", "clientVersion", "SSH-2", },
                { "withHostKeyAlias", "hostKeyAlias", "alias", },
                { "withServerAliveInterval", "serverAliveInterval", 500, },
                { "withServerAliveCountMax", "serverAliveCountMax", 5, },
                { "withIdentityRepository", "identityRepository", new TestIdentityRepository(), },
                { "withIdentities", "identities", Collections.singletonList(Identity.fromFiles(IdentityTest.getPrivateKeyFile())), },
                { "withHostKeyRepository", "hostKeyRepository", new TestHostKeyRepository(), },
                { "withKnownHosts", "knownHosts", new File("known_hosts"), },
                { "withAgentForwarding", "agentForwarding", false, },
                { "withFilenameEncoding", "filenameEncoding", "UTF-8", },
                { "withDefaultDirectory", "defaultDir", "/", },
                { "withClientConnectionCount", "clientConnectionCount", 5, },
                { "withFileSystemExceptionFactory", "fileSystemExceptionFactory", DefaultFileSystemExceptionFactory.INSTANCE, },
                { "withActualTotalSpaceCalculation", "calculateActualTotalSpace", false, },
        };
        return Arrays.asList(parameters);
    }

    @Test
    public void testSetter() throws ReflectiveOperationException {
        SFTPEnvironment env = new SFTPEnvironment();

        assertEquals(Collections.emptyMap(), env);

        setter.invoke(env, propertyValue);

        assertEquals(Collections.singletonMap(propertyName, propertyValue), env);
    }
}
