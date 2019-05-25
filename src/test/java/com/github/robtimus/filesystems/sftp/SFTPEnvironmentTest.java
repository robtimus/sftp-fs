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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.github.robtimus.filesystems.Messages;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.HostKey;
import com.jcraft.jsch.HostKeyRepository;
import com.jcraft.jsch.IdentityRepository;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Proxy;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SocketFactory;
import com.jcraft.jsch.UserInfo;

@SuppressWarnings({ "nls", "javadoc" })
public class SFTPEnvironmentTest {

    @Rule public ExpectedException expectedException = ExpectedException.none();

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

    @Test
    public void testWithIdentity() {
        SFTPEnvironment env = new SFTPEnvironment();

        assertEquals(Collections.emptyMap(), env);

        Identity identity = Identity.fromFiles(IdentityTest.getPrivateKeyFile());

        env.withIdentity(identity);

        assertEquals(Collections.singletonMap("identities", Arrays.asList(identity)), env);
    }

    @Test
    public void testWithIdentities() {
        SFTPEnvironment env = new SFTPEnvironment();

        assertEquals(Collections.emptyMap(), env);

        Identity identity1 = Identity.fromFiles(IdentityTest.getPrivateKeyFile());
        Identity identity2 = Identity.fromFiles(IdentityTest.getPrivateKeyFile(), IdentityTest.getPublicKeyFile(), IdentityTest.getPassphrase());

        env.withIdentities(identity1, identity2);

        assertEquals(Collections.singletonMap("identities", Arrays.asList(identity1, identity2)), env);
    }

    @Test
    public void testInitializeJSchEmpty() throws IOException {
        SFTPEnvironment env = new SFTPEnvironment();

        JSch jsch = mock(JSch.class);
        env.initialize(jsch);

        verifyNoMoreInteractions(jsch);
    }

    @Test
    public void testInitializeJSchFull() throws IOException, JSchException {
        SFTPEnvironment env = new SFTPEnvironment();
        initializeFully(env);

        JSch jsch = mock(JSch.class);
        env.initialize(jsch);

        verify(jsch).setIdentityRepository((IdentityRepository) env.get("identityRepository"));
        verify(jsch).addIdentity(IdentityTest.getPrivateKeyFile().getAbsolutePath());
        verify(jsch).setHostKeyRepository((HostKeyRepository) env.get("hostKeyRepository"));
        verify(jsch).setKnownHosts(((File) env.get("knownHosts")).getAbsolutePath());
        verifyNoMoreInteractions(jsch);
    }

    @Test
    public void testInitializeJSchWithNulls() throws IOException {
        SFTPEnvironment env = new SFTPEnvironment();
        initializeWithNulls(env);

        JSch jsch = mock(JSch.class);
        env.initialize(jsch);

        verify(jsch).setIdentityRepository(null);
        verify(jsch).setHostKeyRepository(null);
        verifyNoMoreInteractions(jsch);
    }

    @Test
    public void testInitializeJSchWithNullIdentity() throws IOException {
        SFTPEnvironment env = new SFTPEnvironment();
        initializeFully(env);
        env.put("identities", Collections.singleton(null));

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(Messages.fileSystemProvider().env().invalidProperty("identities", env.get("identities")).getMessage());

        JSch jsch = mock(JSch.class);
        env.initialize(jsch);
    }

    @Test
    public void testInitializeJSchWithInvalidIdentity() throws IOException {
        SFTPEnvironment env = new SFTPEnvironment();
        initializeFully(env);
        env.put("identities", Collections.singleton("foobar"));

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(Messages.fileSystemProvider().env().invalidProperty("identities", env.get("identities")).getMessage());

        JSch jsch = mock(JSch.class);
        env.initialize(jsch);
    }

    @Test
    public void testInitializeSessionEmpty() throws IOException {
        SFTPEnvironment env = new SFTPEnvironment();

        Session session = mock(Session.class);
        env.initialize(session);

        verifyNoMoreInteractions(session);
    }

    @Test
    public void testInitializeSessionFull() throws IOException, JSchException {
        SFTPEnvironment env = new SFTPEnvironment();
        initializeFully(env);

        Session session = mock(Session.class);
        env.initialize(session);

        verify(session).setProxy((Proxy) env.get("proxy"));
        verify(session).setUserInfo((UserInfo) env.get("userInfo"));
        verify(session).setPassword(new String((char[]) env.get("password")));
        verify(session).setConfig((Properties) env.get("config"));
        verify(session).setSocketFactory((SocketFactory) env.get("socketFactory"));
        verify(session).setTimeout((int) env.get("timeOut"));
        verify(session).setClientVersion((String) env.get("clientVersion"));
        verify(session).setHostKeyAlias((String) env.get("hostKeyAlias"));
        verify(session).setServerAliveInterval((int) env.get("serverAliveInterval"));
        verify(session).setServerAliveCountMax((int) env.get("serverAliveCountMax"));
        verifyNoMoreInteractions(session);
    }

    @Test
    public void testInitializeSessionWithNulls() throws IOException {
        SFTPEnvironment env = new SFTPEnvironment();
        initializeWithNulls(env);

        Session session = mock(Session.class);
        env.initialize(session);

        verify(session).setProxy(null);
        verify(session).setUserInfo(null);
        verify(session).setPassword((String) null);
        verify(session).setSocketFactory(null);
        verify(session).setClientVersion(null);
        verify(session).setHostKeyAlias(null);
        verifyNoMoreInteractions(session);
    }

    @Test
    public void testConnectSessionEmpty() throws IOException, JSchException {
        SFTPEnvironment env = new SFTPEnvironment();

        Session session = mock(Session.class);
        env.connect(session);

        verify(session).connect();
        verify(session).openChannel("sftp");
        verifyNoMoreInteractions(session);
    }

    @Test
    public void testConnectSessionFull() throws IOException, JSchException {
        SFTPEnvironment env = new SFTPEnvironment();
        initializeFully(env);

        Session session = mock(Session.class);
        env.connect(session);

        verify(session).connect((int) env.get("connectTimeout"));
        verify(session).openChannel("sftp");
        verifyNoMoreInteractions(session);
    }

    @Test
    public void testInitializeChannelPreConnectEmpty() throws IOException {
        SFTPEnvironment env = new SFTPEnvironment();

        ChannelSftp channel = mock(ChannelSftp.class);

        env.initializePreConnect(channel);

        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testInitializeChannelPreConnectFull() throws IOException, SftpException {
        SFTPEnvironment env = new SFTPEnvironment();
        initializeFully(env);

        ChannelSftp channel = mock(ChannelSftp.class);

        env.initializePreConnect(channel);

        // can't verify setAgentForwarding because it's not properly mocked
        //verify(channel).setAgentForwarding((boolean) env.get("agentForwarding"));
        verify(channel).setFilenameEncoding((String) env.get("filenameEncoding"));
        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testInitializeChannelPreConnectWithNulls() throws IOException, SftpException {
        SFTPEnvironment env = new SFTPEnvironment();
        initializeWithNulls(env);

        ChannelSftp channel = mock(ChannelSftp.class);

        env.initializePreConnect(channel);

        verify(channel).setFilenameEncoding(null);
        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testConnectChannelEmpty() throws IOException, JSchException {
        SFTPEnvironment env = new SFTPEnvironment();

        ChannelSftp channel = mock(ChannelSftp.class);

        env.connect(channel);

        verify(channel).connect();
        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testConnectChannelFull() throws IOException, JSchException {
        SFTPEnvironment env = new SFTPEnvironment();
        initializeFully(env);

        ChannelSftp channel = mock(ChannelSftp.class);

        env.connect(channel);

        verify(channel).connect((int) env.get("connectTimeout"));
        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testInitializeChannelPostConnectEmpty() throws IOException {
        SFTPEnvironment env = new SFTPEnvironment();

        ChannelSftp channel = mock(ChannelSftp.class);

        env.initializePostConnect(channel);

        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testInitializeChannelPostConnectFull() throws IOException, SftpException {
        SFTPEnvironment env = new SFTPEnvironment();
        initializeFully(env);

        ChannelSftp channel = mock(ChannelSftp.class);

        env.initializePostConnect(channel);

        verify(channel).cd((String) env.get("defaultDir"));
        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testInitializeChannelPostConnectWithNulls() throws IOException {
        SFTPEnvironment env = new SFTPEnvironment();
        initializeWithNulls(env);

        ChannelSftp channel = mock(ChannelSftp.class);

        env.initializePostConnect(channel);

        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testSessionHostKeyRepository() throws JSchException, IOException, ReflectiveOperationException {
        testSessionPropertyInheritedFromJSch("getHostKeyRepository", "hostKeyRepository");
    }

    @Test
    public void testSessionIdentityRepository() throws JSchException, IOException, ReflectiveOperationException {
        testSessionPropertyInheritedFromJSch("getIdentityRepository", "identityRepository");
    }

    private void testSessionPropertyInheritedFromJSch(String getterName, String propertyName)
            throws IOException, JSchException, ReflectiveOperationException {

        SFTPEnvironment env = new SFTPEnvironment();
        initializeFully(env);
        // by adding an identity, the identity repository gets overwritten by a wrapper
        env.remove("identities");

        JSch jsch = new JSch();
        env.initialize(jsch);

        Session session = jsch.getSession("localhost");
        env.initialize(session);

        Method method = Session.class.getDeclaredMethod(getterName);
        method.setAccessible(true);

        assertEquals(env.get(propertyName), method.invoke(session));
    }

    @Test
    public void testWithClientConnectionWaitTimeoutWithUnit() {
        SFTPEnvironment env = new SFTPEnvironment();

        assertEquals(Collections.emptyMap(), env);

        env.withClientConnectionWaitTimeout(1, TimeUnit.MINUTES);

        assertEquals(Collections.singletonMap("clientConnectionWaitTimeout", 60_000L), env);
    }

    private void initializeFully(SFTPEnvironment env) {
        env.withUsername(UUID.randomUUID().toString());
        env.withConnectTimeout(1000);
        env.withProxy(new ProxyHTTP("localhost"));
        env.withUserInfo(new SimpleUserInfo(UUID.randomUUID().toString().toCharArray()));
        env.withPassword(UUID.randomUUID().toString().toCharArray());
        env.withConfig(System.getProperties());
        env.withSocketFactory(new TestSocketFactory());
        env.withTimeout(1000);
        env.withClientVersion("SSH-2");
        env.withHostKeyAlias("alias");
        env.withServerAliveInterval(500);
        env.withServerAliveCountMax(5);
        env.withIdentityRepository(new TestIdentityRepository());
        env.withIdentity(Identity.fromFiles(IdentityTest.getPrivateKeyFile()));
        env.withHostKeyRepository(new TestHostKeyRepository());
        env.withKnownHosts(new File("known_hosts"));
        env.withAgentForwarding(false);
        env.withFilenameEncoding("UTF-8");
        env.withDefaultDirectory("/");
        env.withClientConnectionCount(5);
        env.withFileSystemExceptionFactory(DefaultFileSystemExceptionFactory.INSTANCE);
    }

    private void initializeWithNulls(SFTPEnvironment env) {
        env.withUsername(null);
        env.withProxy(null);
        env.withUserInfo(null);
        env.withPassword(null);
        env.withSocketFactory(null);
        env.withClientVersion(null);
        env.withHostKeyAlias(null);
        env.withIdentityRepository(null);
        env.withHostKeyRepository(null);
        env.withFilenameEncoding(null);
        env.withDefaultDirectory(null);
        env.withFileSystemExceptionFactory(null);
    }

    static final class TestSocketFactory implements SocketFactory {

        @Override
        public Socket createSocket(String host, int port) {
            return null;
        }

        @Override
        public InputStream getInputStream(Socket socket) {
            return null;
        }

        @Override
        public OutputStream getOutputStream(Socket socket) {
            return null;
        }
    }

    static final class TestIdentityRepository implements IdentityRepository {

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

    static final class TestHostKeyRepository implements HostKeyRepository {

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
