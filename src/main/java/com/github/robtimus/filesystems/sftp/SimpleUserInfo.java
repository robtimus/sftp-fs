/*
 * SimpleUserInfo.java
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

import java.util.Objects;
import com.jcraft.jsch.HostKeyRepository;
import com.jcraft.jsch.UserInfo;

/**
 * A simple {@link UserInfo} implementation that always returns the same passphrase and password.
 * It will not show any messages, and {@link #promptYesNo(String)} will always return {@code false}.
 * That means that it will not prompt if the authenticity of a host cannot be established, but will automatically answer the question with "no".
 * Make sure to use a {@link HostKeyRepository} that can verify the host automatically.
 *
 * @author Rob Spoor
 */
public class SimpleUserInfo implements UserInfo {

    private final String passphrase;
    private final char[] password;

    /**
     * Creates a new {@code SimpleUserInfo} object with no passphrase.
     *
     * @param password The password to use.
     * @throws NullPointerException If the password is {@code null}.
     */
    public SimpleUserInfo(char[] password) {
        this(null, password);
    }

    /**
     * Creates a new {@code SimpleUserInfo} object.
     *
     * @param passphrase The passphrase to use.
     * @param password The password to use.
     * @throws NullPointerException If the password is {@code null}.
     */
    public SimpleUserInfo(String passphrase, char[] password) {
        this.passphrase = passphrase;
        this.password = Objects.requireNonNull(password);
    }

    @Override
    public String getPassphrase() {
        return passphrase;
    }

    @Override
    public String getPassword() {
        return new String(password);
    }

    @Override
    public boolean promptPassword(String message) {
        return true;
    }

    @Override
    public boolean promptPassphrase(String message) {
        return passphrase != null;
    }

    @Override
    public boolean promptYesNo(String message) {
        return false;
    }

    @Override
    public void showMessage(String message) {
        // does nothing
    }
}
