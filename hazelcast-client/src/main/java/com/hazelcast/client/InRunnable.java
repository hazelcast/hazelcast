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

import com.hazelcast.impl.ClusterOperation;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;

public class InRunnable extends IORunnable implements Runnable {
    final PacketReader reader;
    Connection connection = null;
    private final OutRunnable outRunnable;

    public InRunnable(HazelcastClient client, OutRunnable outRunnable, Map<Long, Call> calls, PacketReader reader) {
        super(client, calls);
        this.outRunnable = outRunnable;
        this.reader = reader;
    }

    protected void customRun() {
        Packet packet;
        try {
            Connection oldConnection = connection;
            connection = client.connectionManager.getConnection();
            if (oldConnection == null || restoredConnection(oldConnection, connection)) {
                logger.log(Level.FINEST, "restoredConnection");
            	outRunnable.sendReconnectCall();
            	if (oldConnection != null) {
                    redoUnfinishedCalls(oldConnection);
                }
            }
            if (connection == null) {
                outRunnable.clusterIsDown(oldConnection);
                Thread.sleep(50);
            } else {
                packet = reader.readPacket(connection);
                logger.log(Level.FINEST, "Reading " + packet.getOperation() + " Call id: " + packet.getCallId());
                Call call = callMap.remove(packet.getCallId());
                if (call != null) {
                    call.setResponse(packet);
                } else {
                    if (packet.getOperation().equals(ClusterOperation.EVENT)) {
                        client.getListenerManager().enqueue(packet);
                    }
                    if (packet.getCallId() != -1) {
                        logger.log(Level.SEVERE, "In Thread can not handle: " + packet.getOperation() + " : " + packet.getCallId());
                    }
                }
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Throwable e) {
            logger.log(Level.FINE, "InRunnable [" + connection + "] got an exception:" + e.toString());
            outRunnable.clusterIsDown(connection);
        }
    }
    
    private void redoUnfinishedCalls(Connection oldConnection) {
        onDisconnect(oldConnection);
    }
    
    public void shutdown() {
        synchronized (monitor) {
            if (running) {
                this.running = false;
                try {
                    Connection connection = client.connectionManager.getConnection();
                    if (connection != null) {
                        connection.close();
                    }
                } catch (IOException ignored) {
                }
                try {
                    monitor.wait(5000L);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }
}
