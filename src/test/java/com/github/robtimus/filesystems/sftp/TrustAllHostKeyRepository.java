/*
 * TrustAllHostKeyRepository.java
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

import com.jcraft.jsch.HostKey;
import com.jcraft.jsch.HostKeyRepository;
import com.jcraft.jsch.UserInfo;

/**
 * A host key repository that trusts all hosts regardless of the key.
 *
 * @author Rob Spoor
 */
final class TrustAllHostKeyRepository implements HostKeyRepository {

    public static final TrustAllHostKeyRepository INSTANCE = new TrustAllHostKeyRepository();

    private static final HostKey[] NO_HOST_KEYS = {};

    private TrustAllHostKeyRepository() {
        // ignore
    }

    @Override
    public int check(String host, byte[] key) {
        return HostKeyRepository.OK;
    }

    @Override
    public void add(HostKey hostkey, UserInfo ui) {
        // skip
    }

    @Override
    public void remove(String host, String type) {
        // skip
    }

    @Override
    public void remove(String host, String type, byte[] key) {
        // skip
    }

    @Override
    public String getKnownHostsRepositoryID() {
        return "TrustAll"; //$NON-NLS-1$
    }

    @Override
    public HostKey[] getHostKey() {
        return NO_HOST_KEYS;
    }

    @Override
    public HostKey[] getHostKey(String host, String type) {
        return NO_HOST_KEYS;
    }
}
