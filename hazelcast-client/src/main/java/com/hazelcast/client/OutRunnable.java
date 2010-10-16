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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class OutRunnable extends IORunnable {
    final PacketWriter writer;
    final BlockingQueue<Call> queue = new LinkedBlockingQueue<Call>();
    
    private Connection connection = null;
    private static final Call RECONNECT_CALL = new Call(0L, new Packet()); 
    
    volatile Collection<Call> reconnectionCalls;
    private volatile boolean reconnection;

    ILogger logger = Logger.getLogger(this.getClass().getName());

    public OutRunnable(final HazelcastClient client, final Map<Long, Call> calls, final PacketWriter writer) {
        super(client, calls);
        this.writer = writer;
        this.reconnection = false;
    }

    protected void customRun() throws InterruptedException {
        Call call = queue.poll(100, TimeUnit.MILLISECONDS);
        try {
            if (call == null) return;
            int count = 0;
            while (call != null) {
                if (call != RECONNECT_CALL){
                    callMap.put(call.getId(), call);
                }
                Connection oldConnection = connection;
                connection = client.getConnectionManager().getConnection();
                if (restoredConnection(oldConnection, connection)) {
                    resubscribe(call, oldConnection);
                } else if (connection != null) {
                    if (call != RECONNECT_CALL){
                        logger.log(Level.FINEST, "Sending: " + call);
                        writer.write(connection, call.getRequest());
                    }
                } else {
                    clusterIsDown();
                }
                if (reconnectionCalls != null){
                    break;
                }
                call = null;
                if (count++ < 24) {
                    call = queue.poll();
                }
            }
            if (connection != null) {
                writer.flush(connection);
            }
            if (call != null && reconnectionCalls != null && reconnectionCalls.contains(call)) {
                Object response = null;
                for(int i = 0; i < 20 && response == null; i++) {
                    response = call.getResponse(500, TimeUnit.MILLISECONDS);
                }
                if (response != null) {
                    if (reconnectionCalls.remove(call) && reconnectionCalls.isEmpty()){
                        reconnectionCalls = null;
                    }
                } else {
                    logger.log(Level.WARNING, "There is no responce on reconnection call:" + call);
                }
            }
        } catch (Throwable io) {
            logger.log(Level.WARNING, "OutRunnable got exception:" + io.getMessage());
            io.printStackTrace();
            if (call != null){
                enQueue(call);
            }
            client.getConnectionManager().destroyConnection(connection);
        }
    }
    
    void clusterIsDown() {
        interruptWaitingCalls();
        if (!reconnection){
            reconnection = true;
            final Thread thread = new Thread(new Runnable() {
                public void run() {
                    try {
                        if (client.getConnectionManager().lookForAliveConnection() == null){
                            if (reconnection){
                                interruptWaitingCallsAndShutdown();
                            }
                        } else {
                            try {
                                queue.put(RECONNECT_CALL);
                            } catch (InterruptedException e) {
                            }
                        }
                    } catch (IOException e) {
                        logger.log(Level.WARNING, "hz.client.ReconnectionThread got exception:" + e.getMessage(), e);
                    } finally {
                        reconnection = false;
                    }
                }
            });
            thread.setName("hz.client.ReconnectionThread");
            thread.setDaemon(true);
            thread.start();
        }
    }
    
    private void resubscribe(Call call, Connection oldConnection) {
        final BlockingQueue<Call> temp = new LinkedBlockingQueue<Call>();
        queue.drainTo(temp);
        temp.add(call);
        for(final Iterator<Call> it = temp.iterator();it.hasNext();){
            final Call c = it.next();
            if (!callMap.containsKey(c.getId())){
                it.remove();
            }
        }
        reconnectionCalls = new HashSet<Call>(client.getListenerManager().getListenerCalls());
        queue.addAll(reconnectionCalls);
        temp.drainTo(queue);
        onDisconnect(oldConnection);
    }

    public void enQueue(Call call) {
        try {
            if (running == false) {
                throw new NoMemberAvailableException("Client is shutdown either explicitely or implicitely " +
                        "when there is no member available to connect.");
            }
            logger.log(Level.FINEST, "Enqueue: " + call);
            queue.put(call);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public int getQueueSize() {
        return queue.size();
    }
}
