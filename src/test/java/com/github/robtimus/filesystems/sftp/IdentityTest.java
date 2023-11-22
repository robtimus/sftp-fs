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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;

@SuppressWarnings("nls")
class IdentityTest extends AbstractSFTPFileSystemTest {

    private static final File BASE_DIR = new File("src/test/resources", IdentityTest.class.getPackage().getName().replace('.', '/'));

    static final File PRIVATE_KEY_FILE = new File(BASE_DIR, "id_rsa");

    private static final File PUBLIC_KEY_FILE = new File(BASE_DIR, "id_rsa.pub");

    static final String PASSPHRASE_STRING = "1234567890";

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

    @Nested
    class FromFiles {

        @Test
        void testOnlyPrivateKeyFile() throws JSchException {
            Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE);

            JSch jsch = spy(new JSch());

            identity.addIdentity(jsch);

            verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath());
        }

        @Test
        void testPrivateKeyFileAndNullStringPassphrase() throws JSchException {
            Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, (String) null);

            JSch jsch = spy(new JSch());

            identity.addIdentity(jsch);

            verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), (String) null);
        }

        @Test
        void testPrivateKeyFileAndNonNullStringPassphrase() throws JSchException {
            Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, PASSPHRASE_STRING);

            JSch jsch = spy(new JSch());

            identity.addIdentity(jsch);

            verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), PASSPHRASE_STRING);
        }

        @Test
        void testPrivateKeyFileAndNullBytePassphrase() throws JSchException {
            Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, (byte[]) null);

            JSch jsch = spy(new JSch());

            identity.addIdentity(jsch);

            verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), (byte[]) null);
        }

        @Test
        void testPrivateKeyFileAndNonNullBytePassphrase() throws JSchException {
            byte[] passphrase = PASSPHRASE.clone();

            Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, passphrase);

            JSch jsch = spy(new JSch());

            identity.addIdentity(jsch);

            verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), passphrase);
        }

        @Test
        void testPrivateKeyFileNullPublicKeyFileAndNullBytePassphrase() throws JSchException {
            Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, null, null);

            JSch jsch = spy(new JSch());

            identity.addIdentity(jsch);

            verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), null, null);
        }

        @Test
        void testPrivateKeyFileNullPublicKeyFileAndNonNullBytePassphrase() throws JSchException {
            byte[] passphrase = PASSPHRASE.clone();

            Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, null, passphrase);

            JSch jsch = spy(new JSch());

            identity.addIdentity(jsch);

            verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), null, passphrase);
        }

        @Test
        void testPrivateKeyFileNonNullPublicKeyFileAndNullBytePassphrase() throws JSchException {
            Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, PUBLIC_KEY_FILE, null);

            JSch jsch = spy(new JSch());

            identity.addIdentity(jsch);

            verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), PUBLIC_KEY_FILE.getAbsolutePath(), null);
        }

        @Test
        void testPrivateKeyFileNonNullPublicKeyFileAndNonNullBytePassphrase() throws JSchException {
            byte[] passphrase = PASSPHRASE.clone();

            Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, PUBLIC_KEY_FILE, passphrase);

            JSch jsch = spy(new JSch());

            identity.addIdentity(jsch);

            verify(jsch).addIdentity(PRIVATE_KEY_FILE.getAbsolutePath(), PUBLIC_KEY_FILE.getAbsolutePath(), passphrase);
        }
    }

    @Nested
    class FromData {

        @Test
        void testPrivateKeyNullPublicKeyAndNullBytePassphrase() throws JSchException {
            String name = "test";
            byte[] privateKey = PRIVATE_KEY.clone();

            Identity identity = Identity.fromData(name, privateKey, null, null);

            JSch jsch = spy(new JSch());

            identity.addIdentity(jsch);

            verify(jsch).addIdentity(name, privateKey, null, null);

            assertArrayEquals(PRIVATE_KEY, privateKey);
        }

        @Test
        void testPrivateKeyNullPublicKeyAndNonNullBytePassphrase() throws JSchException {
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
        void testPrivateKeyNonNullPublicKeyAndNullBytePassphrase() throws JSchException {
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
        void testPrivateKeyNonNullPublicKeyAndNonNullBytePassphrase() throws JSchException {
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
    }

    @Nested
    class Login {

        @Nested
        class FromFiles {

            @Test
            void testOnlyPrivateKeyFile() throws IOException {
                Identity identity = Identity.fromFiles(PRIVATE_KEY_NOPASS_FILE);

                testLoginSuccess(identity);
            }

            @Test
            void testPrivateKeyFileAndNullStringPassphrase() throws IOException {
                Identity identity = Identity.fromFiles(PRIVATE_KEY_NOPASS_FILE, (String) null);

                testLoginSuccess(identity);
            }

            @Test
            void testPrivateKeyFileAndNonNullStringPassphrase() throws IOException {
                Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, PASSPHRASE_STRING);

                testLoginSuccess(identity);
            }

            @Test
            void testPrivateKeyFileAndNullBytePassphrase() throws IOException {
                Identity identity = Identity.fromFiles(PRIVATE_KEY_NOPASS_FILE, (byte[]) null);

                testLoginSuccess(identity);
            }

            @Test
            void testPrivateKeyFileNullPublicKeyFileAndNullBytePassphrase() throws IOException {
                Identity identity = Identity.fromFiles(PRIVATE_KEY_NOPASS_FILE, null, null);

                testLoginSuccess(identity);
            }

            @Test
            void testPrivateKeyFileNullPublicKeyFileAndNonNullBytePassphrase() throws IOException {
                byte[] passphrase = PASSPHRASE.clone();

                Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, null, passphrase);

                testLoginSuccess(identity);
            }

            @Test
            void testPrivateKeyFileNonNullPublicKeyFileAndNullBytePassphrase() throws IOException {
                Identity identity = Identity.fromFiles(PRIVATE_KEY_NOPASS_FILE, PUBLIC_KEY_FILE_NOPASS, null);

                testLoginSuccess(identity);
            }

            @Test
            void testPrivateKeyFileNonNullPublicKeyFileAndNonNullBytePassphrase() throws IOException {
                byte[] passphrase = PASSPHRASE.clone();

                Identity identity = Identity.fromFiles(PRIVATE_KEY_NOPASS_FILE, PUBLIC_KEY_FILE_NOPASS, passphrase);

                testLoginSuccess(identity);
            }

            @Test
            void testWrongPassphrase() {
                Identity identity = Identity.fromFiles(PRIVATE_KEY_FILE, PUBLIC_KEY_FILE, null);

                testLoginFailure(identity);
            }
        }

        @Nested
        class FromData {

            @Test
            void testNullPublicKeyAndNullPassphrase() throws IOException {
                String name = "test";
                byte[] privateKey = PRIVATE_KEY_NOPASS.clone();

                Identity identity = Identity.fromData(name, privateKey, null, null);

                testLoginSuccess(identity);
            }

            @Test
            void testNullPublicKeyAndNonNullPassphrase() throws IOException {
                String name = "test";
                byte[] privateKey = PRIVATE_KEY.clone();
                byte[] passphrase = PASSPHRASE.clone();

                Identity identity = Identity.fromData(name, privateKey, null, passphrase);

                testLoginSuccess(identity);
            }

            @Test
            void testNonNullPublicKeyAndNullPassphrase() throws IOException {
                String name = "test";
                byte[] privateKey = PRIVATE_KEY_NOPASS.clone();
                byte[] publicKey = PUBLIC_KEY_NOPASS.clone();

                Identity identity = Identity.fromData(name, privateKey, publicKey, null);

                testLoginSuccess(identity);
            }

            @Test
            void testNonNullPublicKeyAndNonNullPassphrase() throws IOException {
                String name = "test";
                byte[] privateKey = PRIVATE_KEY.clone();
                byte[] publicKey = PUBLIC_KEY.clone();
                byte[] passphrase = PASSPHRASE.clone();

                Identity identity = Identity.fromData(name, privateKey, publicKey, passphrase);

                testLoginSuccess(identity);
            }

            @Test
            void testWrongPassphrase() {
                String name = "test";
                byte[] privateKey = PRIVATE_KEY.clone();
                byte[] publicKey = PUBLIC_KEY.clone();

                Identity identity = Identity.fromData(name, privateKey, publicKey, null);

                testLoginFailure(identity);
            }
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
}
