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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.Vector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import com.jcraft.jsch.HostKey;
import com.jcraft.jsch.HostKeyRepository;
import com.jcraft.jsch.IdentityRepository;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.SocketFactory;
import com.jcraft.jsch.UserInfo;

@RunWith(Parameterized.class)
@SuppressWarnings({ "nls", "javadoc" })
public class SFTPEnvironmentSetterTest {

    private final Method setter;
    private final String propertyName;
    private final Object propertyValue;

    public SFTPEnvironmentSetterTest(String methodName, String propertyName, Object propertyValue) {
        this.setter = findMethod(methodName);
        this.propertyName = propertyName;
        this.propertyValue = propertyValue;
    }

    private Method findMethod(String methodName) {
        for (Method method : SFTPEnvironment.class.getMethods()) {
            if (method.getName().equals(methodName) && method.getParameterTypes().length == 1) {
                return method;
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
                { "withHostKeyRepository", "hostKeyRepository", new TestHostKeyRepository(), },
                { "withIdentity", "identity", new File("id_rsa.pub"), },
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

    private static final class TestSocketFactory implements SocketFactory {

        @Override
        public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
            return null;
        }

        @Override
        public InputStream getInputStream(Socket socket) throws IOException {
            return null;
        }

        @Override
        public OutputStream getOutputStream(Socket socket) throws IOException {
            return null;
        }
    }

    private static final class TestIdentityRepository implements IdentityRepository {

        @Override
        public String getName() {
            return null;
        }

        @Override
        public int getStatus() {
            return 0;
        }

        @Override
        public Vector<?> getIdentities() {
            return null;
        }

        @Override
        public boolean add(byte[] identity) {
            return false;
        }

        @Override
        public boolean remove(byte[] blob) {
            return false;
        }

        @Override
        public void removeAll() {
            // does nothing
        }
    }

    private static final class TestHostKeyRepository implements HostKeyRepository {

        @Override
        public int check(String host, byte[] key) {
            return 0;
        }

        @Override
        public void add(HostKey hostkey, UserInfo ui) {
            // does nothing
        }

        @Override
        public void remove(String host, String type) {
            // does nothing
        }

        @Override
        public void remove(String host, String type, byte[] key) {
            // does nothing
        }

        @Override
        public String getKnownHostsRepositoryID() {
            return null;
        }

        @Override
        public HostKey[] getHostKey() {
            return null;
        }

        @Override
        public HostKey[] getHostKey(String host, String type) {
            return null;
        }
    }
}
