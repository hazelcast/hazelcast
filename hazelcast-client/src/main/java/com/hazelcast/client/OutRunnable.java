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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class OutRunnable extends IORunnable {
    final PacketWriter writer;
    public final BlockingQueue<Call> queue = new LinkedBlockingQueue<Call>();
    final BlockingQueue<Call> temp = new LinkedBlockingQueue<Call>();
    private Connection connection = null;
    AtomicInteger counter = new AtomicInteger(0);

    ILogger logger = Logger.getLogger(this.getClass().toString());

    public OutRunnable(final HazelcastClient client, final Map<Long, Call> calls, final PacketWriter writer) {
        super(client, calls);
        this.writer = writer;
    }

    protected void customRun() throws InterruptedException {
        Call call = queue.poll(100, TimeUnit.MILLISECONDS);
        try {
            if (call == null) return;
            int count = 0;
            while (call != null) {
                counter.incrementAndGet();
                callMap.put(call.getId(), call);
                Connection oldConnection = connection;
                connection = client.getConnectionManager().getConnection();
                if (restoredConnection(oldConnection, connection)) {
                    redoUnfinishedCalls(call, oldConnection);
                } else if (connection != null) {
                    logger.log(Level.FINEST, "Sending: " + call);
                    writer.write(connection, call.getRequest());
                } else {
                    interruptWaitingCalls();
                }
                call = null;
                if (count++ < 24) {
                    call = queue.poll();
                }
            }
            writer.flush(connection);
        } catch (Throwable io) {
            logger.log(Level.FINE, "OutRunnable got exception:" + io.getMessage());
            io.printStackTrace();
            enQueue(call);
            client.getConnectionManager().destroyConnection(connection);
        }
    }

    private void redoUnfinishedCalls(Call call, Connection oldConnection) {
        temp.add(call);
        queue.drainTo(temp);
        client.getListenerManager().getListenerCalls().drainTo(queue);
        temp.drainTo(queue);
        onDisconnect(oldConnection);
    }

    public void enQueue(Call call) {
        try {
            logger.log(Level.FINEST, "Enqueue: " + call);
            queue.put(call);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
