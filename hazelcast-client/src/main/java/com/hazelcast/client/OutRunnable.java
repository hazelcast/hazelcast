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

package com.hazelcast.client;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class OutRunnable extends IORunnable {
    final PacketWriter writer;
    final BlockingQueue<Call> queue = new LinkedBlockingQueue<Call>();
    final BlockingQueue<Call> temp = new LinkedBlockingQueue<Call>();
    private Connection connection = null;

    Logger logger = Logger.getLogger(this.getClass().toString());

    public OutRunnable(final HazelcastClient client, final Map<Long, Call> calls, final PacketWriter writer) {
        super(client, calls);
        this.writer = writer;
    }

    protected void customRun() throws InterruptedException {
        Call call = null;
        try {
            call = queue.poll(100, TimeUnit.MILLISECONDS);
            if (call == null) {
                return;
            }
//			System.out.println("Sending: "+call + " " + call.getRequest().getOperation());
            callMap.put(call.getId(), call);
            boolean oldConnectionIsNotNull = (connection != null);
            long oldConnectionId = -1;
            if (oldConnectionIsNotNull) {
                oldConnectionId = connection.getVersion();
            }
            connection = client.connectionManager.getConnection();
            if (restoredConnection(connection, oldConnectionIsNotNull, oldConnectionId)) {
                temp.add(call);
                queue.drainTo(temp);
                client.listenerManager.getListenerCalls().drainTo(queue);
                temp.drainTo(queue);
            } else {
                if (connection != null) {
                    writer.write(connection, call.getRequest());
                } else {
                    interruptWaitingCalls();
                }
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Throwable io) {
            logger.info("OutRunnable got an exception:" + io.getMessage());
            enQueue(call);
            client.connectionManager.destroyConnection(connection);
        }
    }

    private boolean restoredConnection(Connection connection, boolean oldConnectionIsNotNull, long oldConnectionId) {
        return oldConnectionIsNotNull && connection != null && connection.getVersion() != oldConnectionId;
    }

    public void enQueue(Call call) {
        try {
            queue.put(call);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
