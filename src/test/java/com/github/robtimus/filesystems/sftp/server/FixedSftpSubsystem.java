/*
 * FixedSftpSubsystem.java
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

package com.github.robtimus.filesystems.sftp.server;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.sshd.common.util.GenericUtils;
import org.apache.sshd.common.util.io.IoUtils;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.subsystem.sftp.AbstractSftpEventListenerAdapter;
import org.apache.sshd.server.subsystem.sftp.DirectoryHandle;
import org.apache.sshd.server.subsystem.sftp.Handle;
import org.apache.sshd.server.subsystem.sftp.SftpEventListener;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystem;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;
import org.apache.sshd.server.subsystem.sftp.UnsupportedAttributePolicy;

/**
 * {@link SftpSubsystem} does not follow links when the {@code stat} command is executed. This class fixes that.
 *
 * @author Rob Spoor
 */
@SuppressWarnings("javadoc")
public class FixedSftpSubsystem extends SftpSubsystem {

    public FixedSftpSubsystem(ExecutorService executorService, boolean shutdownOnExit, UnsupportedAttributePolicy policy) {
        super(executorService, shutdownOnExit, policy);
    }

    @Override
    protected Map<String, Object> doStat(int id, String path, int flags) throws IOException {
        Path p = resolveFile(path);
        return resolveFileAttributes(p, flags, IoUtils.getLinkOptions(true));
    }

    public static final class Factory extends SftpSubsystemFactory {

        @Override
        public Command create() {
            SftpSubsystem subsystem = new FixedSftpSubsystem(getExecutorService(), isShutdownOnExit(), getUnsupportedAttributePolicy());
            Collection<? extends SftpEventListener> listeners = getRegisteredListeners();
            if (GenericUtils.size(listeners) > 0) {
                for (SftpEventListener l : listeners) {
                    subsystem.addSftpEventListener(l);
                }
            }

            return subsystem;
        }
    }

    public static final class FactoryWithoutSystemDirs extends SftpSubsystemFactory {

        @Override
        public Command create() {
            SftpSubsystem subsystem = new FixedSftpSubsystem(getExecutorService(), isShutdownOnExit(), getUnsupportedAttributePolicy());
            Collection<? extends SftpEventListener> listeners = getRegisteredListeners();
            if (GenericUtils.size(listeners) > 0) {
                for (SftpEventListener l : listeners) {
                    subsystem.addSftpEventListener(l);
                }
            }
            subsystem.addSftpEventListener(new AbstractSftpEventListenerAdapter() {
                @Override
                public void open(ServerSession session, String remoteHandle, Handle localHandle) {
                    if (localHandle instanceof DirectoryHandle) {
                        DirectoryHandle directoryHandle = (DirectoryHandle) localHandle;
                        directoryHandle.markDotSent();
                        directoryHandle.markDotDotSent();
                    }
                }
            });

            return subsystem;
        }
    }
}
