/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.connection;

import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.cluster.client.ClientPingRequest;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class HeartBeatChecker {

    private static final ILogger logger = Logger.getLogger(HeartBeatChecker.class);

    private final int connectionTimeout;
    private final ClientExecutionService executionService;
    private final Data ping;

    public HeartBeatChecker(int timeout, SerializationService serializationService, ClientExecutionService executionService) {
        connectionTimeout = timeout;
        this.executionService = executionService;
        ping = serializationService.toData(new ClientPingRequest());
    }

    public boolean checkHeartBeat(final Connection connection) {
        if ((Clock.currentTimeMillis() - connection.getLastReadTime()) > connectionTimeout / 2) {
            final CountDownLatch latch = new CountDownLatch(1);
            executionService.execute(new Runnable() {
                public void run() {
                    try {
                        connection.write(ping);
                        connection.read();
                        latch.countDown();
                    } catch (IOException e) {
                        logger.severe("Error during heartbeat check!", e);
                    }
                }
            });
            try {
                return latch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                return false;
            }
        } else {
            return true;
        }
    }
}
