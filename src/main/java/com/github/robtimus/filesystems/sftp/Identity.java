/*
 * Identity.java
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

import java.io.File;
import java.util.Objects;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;

/**
 * A combination of a private and public key.
 *
 * @author Rob Spoor
 * @since 1.2
 */
public abstract class Identity {

    private Identity() {
        // private constructor to prevent direct initialization
    }

    abstract void addIdentity(JSch jsch) throws JSchException;

    /**
     * Creates a key pair from private key and public key files.
     * The public key file will be the private key file with {@code .pub} appended to the name.
     *
     * @param privateKeyFile The private key file.
     * @return The created key pair.
     */
    public static Identity fromFiles(final File privateKeyFile) {
        Objects.requireNonNull(privateKeyFile);
        return new Identity() {

            @Override
            void addIdentity(JSch jsch) throws JSchException {
                jsch.addIdentity(privateKeyFile.getAbsolutePath());
            }
        };
    }

    /**
     * Creates a key pair from private key and public key files.
     * The public key file will be the private key file with {@code .pub} appended to the name.
     *
     * @param privateKeyFile The private key file.
     * @param passphrase The passphrase for the private key.
     * @return The created key pair.
     */
    public static Identity fromFiles(final File privateKeyFile, final String passphrase) {
        Objects.requireNonNull(privateKeyFile);
        return new Identity() {

            @Override
            void addIdentity(JSch jsch) throws JSchException {
                jsch.addIdentity(privateKeyFile.getAbsolutePath(), passphrase);
            }
        };
    }

    /**
     * Creates a key pair from private key and public key files.
     * The public key file will be the private key file with {@code .pub} appended to the name.
     *
     * @param privateKeyFile The private key file.
     * @param passphrase The passphrase for the private key.
     * @return The created key pair.
     */
    public static Identity fromFiles(final File privateKeyFile, final byte[] passphrase) {
        Objects.requireNonNull(privateKeyFile);
        return new Identity() {

            @Override
            void addIdentity(JSch jsch) throws JSchException {
                jsch.addIdentity(privateKeyFile.getAbsolutePath(), passphrase);
            }
        };
    }

    /**
     * Creates a key pair from private key and public key files.
     *
     * @param privateKeyFile The private key file.
     * @param publicKeyFile The public key file.
     * @param passphrase The passphrase for the private key.
     * @return The created key pair.
     */
    public static Identity fromFiles(final File privateKeyFile, final File publicKeyFile, final byte[] passphrase) {
        Objects.requireNonNull(privateKeyFile);
        return new Identity() {

            @Override
            void addIdentity(JSch jsch) throws JSchException {
                jsch.addIdentity(privateKeyFile.getAbsolutePath(), publicKeyFile == null ? null : publicKeyFile.getAbsolutePath(), passphrase);
            }
        };
    }

    /**
     * Creates a key pair from a private key and public key.
     *
     * @param name The name of the key pair.
     * @param privateKey The private key data.
     * @param publicKey The public key data.
     * @param passphrase The passphrase for the private key.
     * @return The created key pair.
     * @since 1.4
     */
    public static Identity fromData(final String name, final byte[] privateKey, final byte[] publicKey, final byte[] passphrase) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(privateKey);
        return new Identity() {

            @Override
            void addIdentity(JSch jsch) throws JSchException {
                jsch.addIdentity(name, privateKey, publicKey, passphrase);
            }
        };
    }
}
