/*
* Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
*
*/

package com.hazelcast.client.connection;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.concurrent.CountDownLatch;

public class HeartBeatChecker {

    private final ProtocolWriter writer;
    private final ProtocolReader reader;
    private final int connectionTimeout;

    public HeartBeatChecker(ClientConfig config, SerializationService serializationService) {
        connectionTimeout = config.getConnectionTimeout();
        writer = new ProtocolWriter(serializationService);
        reader = new ProtocolReader(serializationService);
    }

    public boolean checkHeartBeat(final Connection connection) {
        final CountDownLatch latch = new CountDownLatch(1);
        /*if ((System.currentTimeMillis() - connection.getLastRead()) > connectionTimeout / 2) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        writer.write(connection, ping);
                        writer.flush(connection);
                        Protocol pong = reader.read(connection);
                        result.set(pong);
                        latch.countDown();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }, "heartbeat" + connection.getEndpoint());
            thread.start();
            try {
                latch.await(connectionTimeout, TimeUnit.MILLISECONDS);
                return Command.OK.equals(result.get().command);
            } catch (InterruptedException e) {
                return false;
            }
        } else */return true;
    }
}
