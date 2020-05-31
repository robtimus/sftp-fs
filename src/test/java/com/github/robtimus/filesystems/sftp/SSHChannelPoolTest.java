/*
 * SSHChannelPoolTest.java
 * Copyright 2019 Rob Spoor
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
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import com.github.robtimus.filesystems.sftp.SSHChannelPool.Channel;

@SuppressWarnings("javadoc")
public class SSHChannelPoolTest extends AbstractSFTPFileSystemTest {

    @Test
    public void testGetWithTimeout() throws Exception {
        final int clientCount = 3;

        URI uri = getURI();
        SFTPEnvironment env = createEnv()
                .withClientConnectionCount(clientCount)
                .withClientConnectionWaitTimeout(500, TimeUnit.MILLISECONDS);

        SSHChannelPool pool = new SSHChannelPool(uri.getHost(), uri.getPort(), env);
        List<Channel> channels = new ArrayList<>();
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
                // exhaust all available clients
                claimChannels(pool, clientCount, channels);

                long startTime = System.currentTimeMillis();
                IOException exception = assertThrows(IOException.class, () -> claimChannel(pool));
                assertEquals(SFTPMessages.clientConnectionWaitTimeoutExpired(), exception.getMessage());
                assertThat(startTime, lessThanOrEqualTo(System.currentTimeMillis() - 500));
            });
        } finally {
            pool.close();
            for (Channel channel : channels) {
                channel.close();
            }
        }
    }

    @SuppressWarnings("resource")
    private void claimChannels(SSHChannelPool pool, int clientCount, List<Channel> channels) throws IOException {
        for (int i = 0; i < clientCount; i++) {
            channels.add(pool.get());
        }
    }

    @SuppressWarnings("resource")
    private void claimChannel(SSHChannelPool pool) throws IOException {
        pool.get();
    }
}
