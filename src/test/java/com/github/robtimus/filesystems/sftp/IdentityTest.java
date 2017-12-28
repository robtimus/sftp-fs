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

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.io.File;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;

@SuppressWarnings({ "nls", "javadoc" })
public class IdentityTest {

    private static final File BASE_DIR = new File("src/test/resources", IdentityTest.class.getPackage().getName().replace('.', '/'));

    static File getPrivateKeyFile() {
        return new File(BASE_DIR, "id_rsa");
    }

    static File getPublicKeyFile() {
        return new File(BASE_DIR, "id_rsa.pub");
    }

    static byte[] getPassphrase() {
        return getPassphraseString().getBytes(StandardCharsets.UTF_8);
    }

    static String getPassphraseString() {
        return "1234567890";
    }

    @Test
    public void testFromFilesOnlyPrivateKeyFile() throws JSchException {
        File privateKeyFile = getPrivateKeyFile();

        Identity identity = Identity.fromFiles(privateKeyFile);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(privateKeyFile.getAbsolutePath());
    }

    @Test
    public void testFromFilesPrivateKeyFileAndStringPassphrase() throws JSchException {
        File privateKeyFile = getPrivateKeyFile();
        String passphrase = getPassphraseString();

        Identity identity = Identity.fromFiles(privateKeyFile, passphrase);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(privateKeyFile.getAbsolutePath(), passphrase);
    }

    @Test
    public void testFromFilesPrivateKeyFileAndNullBytePassphrase() throws JSchException {
        File privateKeyFile = getPrivateKeyFile();
        byte[] passphrase = null;

        Identity identity = Identity.fromFiles(privateKeyFile, passphrase);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(privateKeyFile.getAbsolutePath(), passphrase);
    }

    @Test
    public void testFromFilesPrivateKeyFileAndNonNullBytePassphrase() throws JSchException {
        File privateKeyFile = getPrivateKeyFile();
        byte[] passphrase = getPassphrase();

        Identity identity = Identity.fromFiles(privateKeyFile, passphrase);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(privateKeyFile.getAbsolutePath(), passphrase);
    }

    @Test
    public void testFromFilesPrivateKeyFileNullPublicKeyFileAndNullBytePassphrase() throws JSchException {
        File privateKeyFile = getPrivateKeyFile();
        File publicKeyFile = null;
        byte[] passphrase = null;

        Identity identity = Identity.fromFiles(privateKeyFile, publicKeyFile, passphrase);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(privateKeyFile.getAbsolutePath(), null, passphrase);
    }

    @Test
    public void testFromFilesPrivateKeyFileNullPublicKeyFileAndNonNullBytePassphrase() throws JSchException {
        File privateKeyFile = getPrivateKeyFile();
        File publicKeyFile = null;
        byte[] passphrase = getPassphrase();

        Identity identity = Identity.fromFiles(privateKeyFile, publicKeyFile, passphrase);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(privateKeyFile.getAbsolutePath(), null, passphrase);
    }

    @Test
    public void testFromFilesPrivateKeyFileNonNullPublicKeyFileAndNullBytePassphrase() throws JSchException {
        File privateKeyFile = getPrivateKeyFile();
        File publicKeyFile = getPublicKeyFile();
        byte[] passphrase = null;

        Identity identity = Identity.fromFiles(privateKeyFile, publicKeyFile, passphrase);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(privateKeyFile.getAbsolutePath(), publicKeyFile.getAbsolutePath(), passphrase);
    }

    @Test
    public void testFromFilesPrivateKeyFileNonNullPublicKeyFileAndNonNullBytePassphrase() throws JSchException {
        File privateKeyFile = getPrivateKeyFile();
        File publicKeyFile = getPublicKeyFile();
        byte[] passphrase = getPassphrase();

        Identity identity = Identity.fromFiles(privateKeyFile, publicKeyFile, passphrase);

        JSch jsch = spy(new JSch());

        identity.addIdentity(jsch);

        verify(jsch).addIdentity(privateKeyFile.getAbsolutePath(), publicKeyFile.getAbsolutePath(), passphrase);
    }
}
