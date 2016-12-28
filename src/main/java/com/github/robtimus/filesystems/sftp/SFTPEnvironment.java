/*
 * SFTPEnvironment.java
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

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.nio.file.FileStore;
import java.nio.file.FileSystemException;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import com.github.robtimus.filesystems.FileSystemProviderSupport;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.HostKeyRepository;
import com.jcraft.jsch.IdentityRepository;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Proxy;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SocketFactory;
import com.jcraft.jsch.UserInfo;

/**
 * A utility class to set up environments that can be used in the {@link FileSystemProvider#newFileSystem(URI, Map)} and
 * {@link FileSystemProvider#newFileSystem(Path, Map)} methods of {@link SFTPFileSystemProvider}.
 *
 * @author Rob Spoor
 */
public class SFTPEnvironment implements Map<String, Object>, Cloneable {

    // session support

    private static final String USERNAME = "username"; //$NON-NLS-1$

    // connect support

    private static final String CONNECT_TIMEOUT = "connectTimeout"; //$NON-NLS-1$

    // Session

    private static final String PROXY = "proxy"; //$NON-NLS-1$
    private static final String USER_INFO = "userInfo"; //$NON-NLS-1$
    private static final String PASSWORD = "password"; //$NON-NLS-1$
    private static final String CONFIG = "config"; //$NON-NLS-1$
    private static final String SOCKET_FACTORY = "socketFactory"; //$NON-NLS-1$
    private static final String TIMEOUT = "timeOut"; //$NON-NLS-1$
    private static final String CLIENT_VERSION = "clientVersion"; //$NON-NLS-1$
    private static final String HOST_KEY_ALIAS = "hostKeyAlias"; //$NON-NLS-1$
    private static final String SERVER_ALIVE_INTERVAL = "serverAliveInterval"; //$NON-NLS-1$
    private static final String SERVER_ALIVE_COUNTMAX = "serverAliveCountMax"; //$NON-NLS-1$
    private static final String IDENTITY_REPOSITORY = "identityRepository"; //$NON-NLS-1$
    private static final String HOST_KEY_REPOSITORY = "hostKeyRepository"; //$NON-NLS-1$
    // don't support port forwarding, X11

    // ChannelSession

    private static final String AGENT_FORWARDING = "agentForwarding"; //$NON-NLS-1$
    // don't support X11 forwarding, PTY or TTY

    // ChannelSftp

    private static final String FILENAME_ENCODING = "filenameEncoding"; //$NON-NLS-1$

    // SFTP file system support

    private static final String DEFAULT_DIR = "defaultDir"; //$NON-NLS-1$
    private static final int DEFAULT_CLIENT_CONNECTION_COUNT = 5;
    private static final String CLIENT_CONNECTION_COUNT = "clientConnectionCount"; //$NON-NLS-1$
    private static final String FILE_SYSTEM_EXCEPTION_FACTORY = "fileSystemExceptionFactory"; //$NON-NLS-1$
    private static final String CALCULATE_ACTUAL_TOTAL_SPACE = "calculateActualTotalSpace"; //$NON-NLS-1$

    private Map<String, Object> map;

    /**
     * Creates a new SFTP environment.
     */
    public SFTPEnvironment() {
        map = new HashMap<>();
    }

    /**
     * Creates a new SFTP environment.
     *
     * @param map The map to wrap.
     */
    public SFTPEnvironment(Map<String, Object> map) {
        this.map = Objects.requireNonNull(map);
    }

    @SuppressWarnings("unchecked")
    static SFTPEnvironment wrap(Map<String, ?> map) {
        if (map instanceof SFTPEnvironment) {
            return (SFTPEnvironment) map;
        }
        return new SFTPEnvironment((Map<String, Object>) map);
    }

    // session support

    /**
     * Stores the username to use.
     *
     * @param username The username to use.
     * @return This object.
     */
    public SFTPEnvironment withUsername(String username) {
        put(USERNAME, username);
        return this;
    }

    // connect support

    /**
     * Stores the connection timeout to use.
     *
     * @param timeout The connection timeout in milliseconds.
     * @return This object.
     */
    public SFTPEnvironment withConnectTimeout(int timeout) {
        put(CONNECT_TIMEOUT, timeout);
        return this;
    }

    // Session

    /**
     * Stores the proxy to use.
     *
     * @param proxy The proxy to use.
     * @return This object.
     */
    public SFTPEnvironment withProxy(Proxy proxy) {
        put(PROXY, proxy);
        return this;
    }

    /**
     * Stores the user info to use.
     *
     * @param userInfo The user info to use.
     * @return This object.
     */
    public SFTPEnvironment withUserInfo(UserInfo userInfo) {
        put(USER_INFO, userInfo);
        return this;
    }

    /**
     * Stores the password to use.
     *
     * @param password The password to use.
     * @return This object.
     * @since 1.1
     */
    public SFTPEnvironment withPassword(char[] password) {
        put(PASSWORD, password);
        return this;
    }

    /**
     * Stores configuration options to use. This method will add not clear any previously set options, but only add new ones.
     *
     * @param config The configuration options to use.
     * @return This object.
     */
    public SFTPEnvironment withConfig(Properties config) {
        getConfig().putAll(config);
        return this;
    }

    /**
     * Stores a configuration option to use. This method will add not clear any previously set options, but only add new ones.
     *
     * @param key The configuration key.
     * @param value The configuration value.
     * @return This object.
     */
    public SFTPEnvironment withConfig(String key, String value) {
        getConfig().setProperty(key, value);
        return this;
    }

    private Properties getConfig() {
        Properties config = FileSystemProviderSupport.getValue(this, CONFIG, Properties.class, null);
        if (config == null) {
            config = new Properties();
            put(CONFIG, config);
        }
        return config;
    }

    /**
     * Stores the socket factory to use.
     *
     * @param factory The socket factory to use.
     * @return This object.
     */
    public SFTPEnvironment withSocketFactory(SocketFactory factory) {
        put(SOCKET_FACTORY, factory);
        return this;
    }

    /**
     * Stores the timeout.
     *
     * @param timeout The timeout in milliseconds.
     * @return This object.
     * @see Socket#setSoTimeout(int)
     */
    public SFTPEnvironment withTimeout(int timeout) {
        put(TIMEOUT, timeout);
        return this;
    }

    /**
     * Stores the client version to use.
     *
     * @param version The client version.
     * @return This object.
     */
    public SFTPEnvironment withClientVersion(String version) {
        put(CLIENT_VERSION, version);
        return this;
    }

    /**
     * Stores the host key alias to use.
     *
     * @param alias The host key alias.
     * @return This object.
     */
    public SFTPEnvironment withHostKeyAlias(String alias) {
        put(HOST_KEY_ALIAS, alias);
        return this;
    }

    /**
     * Stores the server alive interval to use.
     *
     * @param interval The server alive interval in milliseconds.
     * @return This object.
     */
    public SFTPEnvironment withServerAliveInterval(int interval) {
        put(SERVER_ALIVE_INTERVAL, interval);
        return this;
    }

    /**
     * Stores the maximum number of server alive messages that can be sent without any reply before disconnecting.
     *
     * @param count The maximum number of server alive messages.
     * @return This object.
     */
    public SFTPEnvironment withServerAliveCountMax(int count) {
        put(SERVER_ALIVE_COUNTMAX, count);
        return this;
    }

    /**
     * Stores the identity repository to use.
     *
     * @param repository The identity repository to use.
     * @return This object.
     */
    public SFTPEnvironment withIdentityRepository(IdentityRepository repository) {
        put(IDENTITY_REPOSITORY, repository);
        return this;
    }

    /**
     * Stores the host key repository to use.
     *
     * @param repository The host key repository to use.
     * @return This object.
     */
    public SFTPEnvironment withHostKeyRepository(HostKeyRepository repository) {
        put(HOST_KEY_REPOSITORY, repository);
        return this;
    }

    /**
     * Stores whether or not agent forwarding should be enabled.
     *
     * @param agentForwarding {@code true} to enable strict agent forwarding, or {@code false} to disable it.
     * @return This object.
     */
    public SFTPEnvironment withAgentForwarding(boolean agentForwarding) {
        put(AGENT_FORWARDING, agentForwarding);
        return this;
    }

    /**
     * Stores the filename encoding to use.
     *
     * @param encoding The filename encoding to use.
     * @return This object.
     */
    public SFTPEnvironment withFilenameEncoding(String encoding) {
        put(FILENAME_ENCODING, encoding);
        return this;
    }

    // SFTP file system support

    /**
     * Stores the default directory to use.
     * If it exists, this will be the directory that relative paths are resolved to.
     *
     * @param pathname The default directory to use.
     * @return This object.
     */
    public SFTPEnvironment withDefaultDirectory(String pathname) {
        put(DEFAULT_DIR, pathname);
        return this;
    }

    /**
     * Stores the number of client connections to use. This value influences the number of concurrent threads that can access an SFTP file system.
     *
     * @param count The number of client connection to use.
     * @return This object.
     */
    public SFTPEnvironment withClientConnectionCount(int count) {
        put(CLIENT_CONNECTION_COUNT, count);
        return this;
    }

    /**
     * Stores the file system exception factory to use.
     *
     * @param factory The file system exception factory to use.
     * @return This object.
     */
    public SFTPEnvironment withFileSystemExceptionFactory(FileSystemExceptionFactory factory) {
        put(FILE_SYSTEM_EXCEPTION_FACTORY, factory);
        return this;
    }

    /**
     * Stores whether or not {@link FileStore#getTotalSpace()} should calculate the actual total space by traversing the file system.
     * If not explicitly set to {@code true}, the method will return {@link Long#MAX_VALUE} instead.
     *
     * @param calculateActualTotalSpace {@code true} if {@link FileStore#getTotalSpace()} should calculate the actual total space by traversing the
     *            file system, or {@code false} otherwise.
     * @return This object.
     * @deprecated {@link FileStore#getTotalSpace()} does not need to traverse the file system, because that would calculate the total <em>used</em>
     *             space, not the total space.
     */
    @Deprecated
    public SFTPEnvironment withActualTotalSpaceCalculation(boolean calculateActualTotalSpace) {
        put(CALCULATE_ACTUAL_TOTAL_SPACE, calculateActualTotalSpace);
        return this;
    }

    String getUsername() {
        return FileSystemProviderSupport.getValue(this, USERNAME, String.class, null);
    }

    int getClientConnectionCount() {
        int count = FileSystemProviderSupport.getIntValue(this, CLIENT_CONNECTION_COUNT, DEFAULT_CLIENT_CONNECTION_COUNT);
        return Math.max(1, count);
    }

    FileSystemExceptionFactory getExceptionFactory() {
        return FileSystemProviderSupport.getValue(this, FILE_SYSTEM_EXCEPTION_FACTORY, FileSystemExceptionFactory.class,
                DefaultFileSystemExceptionFactory.INSTANCE);
    }

    ChannelSftp openChannel(JSch jsch, String hostname, int port) throws IOException {
        Session session = getSession(jsch, hostname, port);
        try {
            initialize(session);
            ChannelSftp channel = connect(session);
            try {
                initializePreConnect(channel);
                connect(channel);
                initializePostConnect(channel);
                verifyConnection(channel);
                return channel;
            } catch (IOException e) {
                channel.disconnect();
                throw e;
            }
        } catch (IOException e) {
            session.disconnect();
            throw e;
        }
    }

    Session getSession(JSch jsch, String hostname, int port) throws IOException {
        String username = getUsername();

        try {
            return jsch.getSession(username, hostname, port == -1 ? 22 : port);
        } catch (JSchException e) {
            throw asFileSystemException(e);
        }
    }

    void initialize(Session session) throws IOException {
        Proxy proxy = FileSystemProviderSupport.getValue(this, PROXY, Proxy.class, null);
        if (proxy != null) {
            session.setProxy(proxy);
        }

        UserInfo userInfo = FileSystemProviderSupport.getValue(this, USER_INFO, UserInfo.class, null);
        if (userInfo != null) {
            session.setUserInfo(userInfo);
        }

        char[] password = FileSystemProviderSupport.getValue(this, PASSWORD, char[].class, null);
        if (password != null) {
            session.setPassword(new String(password));
        }

        Properties config = FileSystemProviderSupport.getValue(this, CONFIG, Properties.class, null);
        if (config != null) {
            session.setConfig(config);
        }

        SocketFactory socketFactory = FileSystemProviderSupport.getValue(this, SOCKET_FACTORY, SocketFactory.class, null);
        if (socketFactory != null) {
            session.setSocketFactory(socketFactory);
        }

        if (get(TIMEOUT) != null) {
            int timeout = FileSystemProviderSupport.getIntValue(this, TIMEOUT);
            try {
                session.setTimeout(timeout);
            } catch (JSchException e) {
                throw asFileSystemException(e);
            }
        }

        String clientVersion = FileSystemProviderSupport.getValue(this, CLIENT_VERSION, String.class, null);
        if (clientVersion != null) {
            session.setClientVersion(clientVersion);
        }

        String hostKeyAlias = FileSystemProviderSupport.getValue(this, HOST_KEY_ALIAS, String.class, null);
        if (hostKeyAlias != null) {
            session.setHostKeyAlias(hostKeyAlias);
        }

        if (get(SERVER_ALIVE_INTERVAL) != null) {
            int interval = FileSystemProviderSupport.getIntValue(this, SERVER_ALIVE_INTERVAL);
            try {
                session.setServerAliveInterval(interval);
            } catch (JSchException e) {
                throw asFileSystemException(e);
            }
        }
        if (get(SERVER_ALIVE_COUNTMAX) != null) {
            int count = FileSystemProviderSupport.getIntValue(this, SERVER_ALIVE_COUNTMAX);
            session.setServerAliveCountMax(count);
        }

        IdentityRepository identityRepository = FileSystemProviderSupport.getValue(this, IDENTITY_REPOSITORY, IdentityRepository.class, null);
        if (identityRepository != null) {
            session.setIdentityRepository(identityRepository);
        }

        HostKeyRepository hostKeyRepository = FileSystemProviderSupport.getValue(this, HOST_KEY_REPOSITORY, HostKeyRepository.class, null);
        if (hostKeyRepository != null) {
            session.setHostKeyRepository(hostKeyRepository);
        }
    }

    ChannelSftp connect(Session session) throws IOException {
        try {
            if (get(CONNECT_TIMEOUT) != null) {
                int connectTimeout = FileSystemProviderSupport.getIntValue(this, CONNECT_TIMEOUT);
                session.connect(connectTimeout);
            } else {
                session.connect();
            }

            return (ChannelSftp) session.openChannel("sftp"); //$NON-NLS-1$
        } catch (JSchException e) {
            throw asFileSystemException(e);
        }
    }

    void initializePreConnect(ChannelSftp channel) throws IOException {
        if (get(AGENT_FORWARDING) != null) {
            boolean forwarding = FileSystemProviderSupport.getBooleanValue(this, AGENT_FORWARDING);
            channel.setAgentForwarding(forwarding);
        }

        String filenameEncoding = FileSystemProviderSupport.getValue(this, FILENAME_ENCODING, String.class, null);
        if (filenameEncoding != null) {
            try {
                channel.setFilenameEncoding(filenameEncoding);
            } catch (SftpException e) {
                throw asFileSystemException(e);
            }
        }
    }

    void connect(ChannelSftp channel) throws IOException {
        try {
            if (get(CONNECT_TIMEOUT) != null) {
                int connectTimeout = FileSystemProviderSupport.getIntValue(this, CONNECT_TIMEOUT);
                channel.connect(connectTimeout);
            } else {
                channel.connect();
            }
        } catch (JSchException e) {
            throw asFileSystemException(e);
        }
    }

    void initializePostConnect(ChannelSftp channel) throws IOException {
        String defaultDir = FileSystemProviderSupport.getValue(this, DEFAULT_DIR, String.class, null);
        if (defaultDir != null) {
            try {
                channel.cd(defaultDir);
            } catch (SftpException e) {
                throw getExceptionFactory().createChangeWorkingDirectoryException(defaultDir, e);
            }
        }
    }

    void verifyConnection(ChannelSftp channel) throws IOException {
        try {
            channel.pwd();
        } catch (SftpException e) {
            throw asFileSystemException(e);
        }
    }

    FileSystemException asFileSystemException(Exception e) throws FileSystemException {
        if (e instanceof FileSystemException) {
            throw (FileSystemException) e;
        }
        FileSystemException exception = new FileSystemException(null, null, e.getMessage());
        exception.initCause(e);
        throw exception;
    }

    // Map / Object

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return map.get(key);
    }

    @Override
    public Object put(String key, Object value) {
        return map.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return map.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        map.putAll(m);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set<String> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<Object> values() {
        return map.values();
    }

    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        return map.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        return map.equals(o);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public String toString() {
        return map.toString();
    }

    @Override
    public SFTPEnvironment clone() {
        try {
            SFTPEnvironment clone = (SFTPEnvironment) super.clone();
            clone.map = new HashMap<>(map);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }
}
