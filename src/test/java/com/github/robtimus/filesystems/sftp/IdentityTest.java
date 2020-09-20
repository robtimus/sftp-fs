/*
 * IdentityTest.java
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessMode;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemException;
import org.junit.jupiter.api.Test;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;

@SuppressWarnings("nls")
class IdentityTest extends AbstractSFTPFileSystemTest {

    private static final File BASE_DIR = new File("src/test/resources", IdentityTest.class.getPackage().getName().replace('.', '/'));

    private static final File PRIVATE_KEY_FILE = new File(BASE_DIR, "id_rsa");

    private static final File PUBLIC_KEY_FILE = new File(BASE_DIR, "id_rsa.pub");

    private static final String PASSPHRASE_STRING = "1234567890";

    private static final byte[] PRIVATE_KEY = readContent(PRIVATE_KEY_FILE);

    private static final byte[] PUBLIC_KEY = readContent(PUBLIC_KEY_FILE);

    private static final byte[] PASSPHRASE = PASSPHRASE_STRING.getBytes(StandardCharsets.UTF_8);

    private static final File PRIVATE_KEY_NOPASS_FILE = new File(BASE_DIR, "id_rsa_nopass");

    private static final File PUBLIC_KEY_FILE_NOPASS = new File(BASE_DIR, "id_rsa_nopass.pub");

    private static final byte[] PRIVATE_KEY_NOPASS = readContent(PRIVATE_KEY_NOPASS_FILE);

    private static final byte[] PUBLIC_KEY_NOPASS = readContent(PUBLIC_KEY_FILE_NOPASS);

    private static byte[] readContent(File file) {
        try (InputStream input = new FileInputStream(file)) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int len;
            while ((len = input.read(buffer)) != -1) {
                output.write(buffer, 0, len);
            }
            return output.toByteArray();

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static Identity fromFiles() {
        return Identity.fromFiles(PRIVATE_KEY_FILE);
    }

    static Identity fromData() {
        return Identity.fromData("test", PRIVATE_KEY.clone(), PUBLIC_KEY.clone(), PASSPHRASE.clone());
    }

    static void assertIdentityFromFilesAdded(JSch jsch) throws JSchException {
        verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath());
    }

    @Test
    void testFromFilesOnlyPrivateKeyFile() throws JSchException {
        Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath());
    }

    @Test
    void testFromFilesPrivateKeyFileAndNullStringPassphrase() throws JSchException {
        Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, (String) null);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), (String) null);
    }

    @Test
    void testFromFilesPrivateKeyFileAndNonNullStringPassphrase() throws JSchException {
        Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, PASSPHRASE_STRING);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), PASSPHRASE_STRING);
    }

    @Test
    void testFromFilesPrivateKeyFileAndNullBytePassphrase() throws JSchException {
        Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, (byte[]) null);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), (byte[]) null);
    }

    @Test
    void testFromFilesPrivateKeyFileAndNonNullBytePassphrase() throws JSchException {
        byte[] passphrase = PASSPHRASE.clone();

        Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, passphrase);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), passphrase);
    }

    @Test
    void testFromFilesPrivateKeyFileNullPublicKeyFileAndNullBytePassphrase() throws JSchException {
        Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, null, null);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), null, null);
    }

    @Test
    void testFromFilesPrivateKeyFileNullPublicKeyFileAndNonNullBytePassphrase() throws JSchException {
        byte[] passphrase = PASSPHRASE.clone();

        Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, null, passphrase);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), null, passphrase);
    }

    @Test
    void testFromFilesPrivateKeyFileNonNullPublicKeyFileAndNullBytePassphrase() throws JSchException {
        Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, PUBLIC_KEY_FILE, null);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), PUBLIC_KEY_FILE.getAbsolutePath(), null);
    }

    @Test
    void testFromFilesPrivateKeyFileNonNullPublicKeyFileAndNonNullBytePassphrase() throws JSchException {
        byte[] passphrase = PASSPHRASE.clone();

        Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, PUBLIC_KEY_FILE, passphrase);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), PUBLIC_KEY_FILE.getAbsolutePath(), passphrase);
    }

    @Test
    void testFromDataPrivateKeyNullPublicKeyAndNullBytePassphrase() throws JSchException {
        String name = "test";
        byte[] privateKey = PRIVATE_KEY.clone();

        Identity identity = Identity.fromData(name, privateKey, null, null);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(name, privateKey, null, null);

        assertArrayEquals(PRIVATE_KEY, privateKey);
    }

    @Test
    void testFromDataPrivateKeyNullPublicKeyAndNonNullBytePassphrase() throws JSchException {
        String name = "test";
        byte[] privateKey = PRIVATE_KEY.clone();
        byte[] passphrase = PASSPHRASE.clone();

        Identity identity = Identity.fromData(name, privateKey, null, passphrase);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(name, privateKey, null, passphrase);

        assertArrayEquals(PRIVATE_KEY, privateKey);
    }

    @Test
    void testFromDataPrivateKeyNonNullPublicKeyAndNullBytePassphrase() throws JSchException {
        String name = "test";
        byte[] privateKey = PRIVATE_KEY.clone();
        byte[] publicKey = PUBLIC_KEY.clone();

        Identity identity = Identity.fromData(name, privateKey, publicKey, null);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(name, privateKey, publicKey, null);

        assertArrayEquals(PRIVATE_KEY, privateKey);
        assertArrayEquals(PUBLIC_KEY, publicKey);
    }

    @Test
    void testFromDataPrivateKeyNonNullPublicKeyAndNonNullBytePassphrase() throws JSchException {
        String name = "test";
        byte[] privateKey = PRIVATE_KEY.clone();
        byte[] publicKey = PUBLIC_KEY.clone();
        byte[] passphrase = PASSPHRASE.clone();

        Identity identity = Identity.fromData(name, privateKey, publicKey, passphrase);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(name, privateKey, publicKey, passphrase);

        assertArrayEquals(PRIVATE_KEY, privateKey);
        assertArrayEquals(PUBLIC_KEY, publicKey);
    }

    @Test
    void testLoginFromFilesOnlyPrivateKeyFile() throws IOException {
        Identity identity = Identity.fromFiles(PRIVATE_KEY_NOPASS_FILE);

        testLoginSuccess(identity);
    }

    @Test
    void testLoginFromFilesPrivateKeyFileAndNullStringPassphrase() throws IOException {
        Identity identity = Identity.fromFiles(PRIVATE_KEY_NOPASS_FILE, (String) null);

        testLoginSuccess(identity);
    }

    @Test
    void testLoginFromFilesPrivateKeyFileAndNonNullStringPassphrase() throws IOException {
        Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, PASSPHRASE_STRING);

        testLoginSuccess(identity);
    }

    @Test
    void testLoginFromFilesPrivateKeyFileAndNullBytePassphrase() throws IOException {
        Identity identity = Identity.fromFiles(PRIVATE_KEY_NOPASS_FILE, (byte[]) null);

        testLoginSuccess(identity);
    }

    @Test
    void testLoginFromFilesPrivateKeyFileNullPublicKeyFileAndNullBytePassphrase() throws IOException {
        Identity identity = Identity.fromFiles(PRIVATE_KEY_NOPASS_FILE, null, null);

        testLoginSuccess(identity);
    }

    @Test
    void testLoginFromFilesPrivateKeyFileNullPublicKeyFileAndNonNullBytePassphrase() throws IOException {
        byte[] passphrase = PASSPHRASE.clone();

        Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, null, passphrase);

        testLoginSuccess(identity);
    }

    @Test
    void testLoginFromFilesPrivateKeyFileNonNullPublicKeyFileAndNullBytePassphrase() throws IOException {
        Identity identity = Identity.fromFiles(PRIVATE_KEY_NOPASS_FILE, PUBLIC_KEY_FILE_NOPASS, null);

        testLoginSuccess(identity);
    }

    @Test
    void testLoginFromFilesPrivateKeyFileNonNullPublicKeyFileAndNonNullBytePassphrase() throws IOException {
        byte[] passphrase = PASSPHRASE.clone();

        Identity identity = Identity.fromFiles(PRIVATE_KEY_NOPASS_FILE, PUBLIC_KEY_FILE_NOPASS, passphrase);

        testLoginSuccess(identity);
    }

    @Test
    void testLoginFromFilesWrongPassphrase() {
        Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, PUBLIC_KEY_FILE, null);

        testLoginFailure(identity);
    }

    @Test
    void testLoginFromDataNullPublicKeyAndNullPassphrase() throws IOException {
        String name = "test";
        byte[] privateKey = PRIVATE_KEY_NOPASS.clone();

        Identity identity = Identity.fromData(name, privateKey, null, null);

        testLoginSuccess(identity);
    }

    @Test
    void testLoginFromDataNullPublicKeyAndNonNullPassphrase() throws IOException {
        String name = "test";
        byte[] privateKey = PRIVATE_KEY.clone();
        byte[] passphrase = PASSPHRASE.clone();

        Identity identity = Identity.fromData(name, privateKey, null, passphrase);

        testLoginSuccess(identity);
    }

    @Test
    void testLoginFromDataNonNullPublicKeyAndNullPassphrase() throws IOException {
        String name = "test";
        byte[] privateKey = PRIVATE_KEY_NOPASS.clone();
        byte[] publicKey = PUBLIC_KEY_NOPASS.clone();

        Identity identity = Identity.fromData(name, privateKey, publicKey, null);

        testLoginSuccess(identity);
    }

    @Test
    void testLoginFromDataNonNullPublicKeyAndNonNullPassphrase() throws IOException {
        String name = "test";
        byte[] privateKey = PRIVATE_KEY.clone();
        byte[] publicKey = PUBLIC_KEY.clone();
        byte[] passphrase = PASSPHRASE.clone();

        Identity identity = Identity.fromData(name, privateKey, publicKey, passphrase);

        testLoginSuccess(identity);
    }

    @Test
    void testLoginFromDataWrongPassphrase() {
        String name = "test";
        byte[] privateKey = PRIVATE_KEY.clone();
        byte[] publicKey = PUBLIC_KEY.clone();

        Identity identity = Identity.fromData(name, privateKey, publicKey, null);

        testLoginFailure(identity);
    }

    private void testLoginSuccess(Identity identity) throws IOException {
        SFTPEnvironment env = createEnv()
                .withUserInfo(null)
                .withIdentity(identity);
        try (FileSystem fileSystem = new SFTPFileSystemProvider().newFileSystem(getURI(), env)) {
            fileSystem.provider().checkAccess(createPath("/"), AccessMode.READ);
        }
    }

    private void testLoginFailure(final Identity identity) {
        SFTPEnvironment env = createEnv()
                .withUserInfo(null)
                .withIdentity(identity);
        FileSystemException exception = assertThrows(FileSystemException.class, () -> new SFTPFileSystemProvider().newFileSystem(getURI(), env));
        assertThat(exception.getCause(), instanceOf(JSchException.class));
        assertEquals("USERAUTH fail", exception.getMessage());
    }
}
