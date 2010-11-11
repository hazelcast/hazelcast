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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
    
    private volatile boolean reconnection;

    private Collection<Call> reconnectionCalls;

    public OutRunnable(final HazelcastClient client, final Map<Long, Call> calls, final PacketWriter writer) {
        super(client, calls);
        this.writer = writer;
        this.reconnection = false;
    }

    protected void customRun() throws InterruptedException {
        Call call = queue.poll(100, TimeUnit.MILLISECONDS);
        try {
            if (call == null) {
                if (reconnectionCalls != null) {
                    checkOnReconnect(call);
                }
                return;
            }
            if (call != RECONNECT_CALL){
                callMap.put(call.getId(), call);
            }
            Connection oldConnection = connection;
            connection = client.getConnectionManager().getConnection();
            boolean wrote = false;
            if (restoredConnection(oldConnection, connection)) {
                resubscribe(call, oldConnection);
            } else if (connection != null) {
                if (call != RECONNECT_CALL){
                    logger.log(Level.FINEST, "Sending: " + call);
                    wrote = true;
                    writer.write(connection, call.getRequest());
                }
            } else {
                clusterIsDown(oldConnection);
            }
            if (connection != null && wrote) {
                writer.flush(connection);
            }
            if (reconnectionCalls != null) {
                checkOnReconnect(call);
            }
        } catch (Throwable io) {
            logger.log(Level.WARNING, "OutRunnable got exception:" + io.getMessage(), io);
            if (call != null){
                enQueue(call);
            }
            clusterIsDown(connection);
        }
    }

    private void checkOnReconnect(Call call) {
        final Collection oldCalls = reconnectionCalls;
        try{
            Object response = reconnectionCalls.contains(call) ? 
                call.getResponse(100L, TimeUnit.MILLISECONDS) :
                null;
            if (response != null){
                reconnectionCalls.remove(call);
            } else {
                for (final Iterator<Call> it = reconnectionCalls.iterator();it.hasNext();) {
                    final Call c = it.next();
                    response = !c.hasResponse() ? c.getResponse(100L, TimeUnit.MILLISECONDS) : Boolean.TRUE;
                    if (response != null){
                        it.remove();
                    }
                }
            }
        } catch (Throwable e){
            // nothing to do
        }
        
        if (reconnectionCalls.isEmpty()){
            reconnectionCalls = null;
        }
        
        if (oldCalls != null && reconnectionCalls == null){
            client.getConnectionManager().notifyConnectionIsOpened();
        }
    }
    
    @Override
    public void interruptWaitingCalls() {
        super.interruptWaitingCalls();
        final BlockingQueue<Call> temp = new LinkedBlockingQueue<Call>();
        queue.drainTo(temp);
        clearCalls(temp);
        clearCalls(reconnectionCalls);
        reconnectionCalls = null;
    }
    
    private void clearCalls(final Collection<Call> calls) {
        if (calls == null) return;
        for(final Iterator<Call> it = calls.iterator();it.hasNext();){
            final Call c = it.next();
            if (c == RECONNECT_CALL) continue;
            c.setResponse(new NoMemberAvailableException());
            it.remove();
        }
    }
    
    void clusterIsDown(Connection oldConnection) {
        client.getConnectionManager().destroyConnection(oldConnection);
        interruptWaitingCalls();
        if (!reconnection){
            reconnection = true;
            final Thread thread = new Thread(new Runnable() {
                public void run() {
                    try {
                        final Connection lookForAliveConnection = client.getConnectionManager().lookForAliveConnection();
                        if (lookForAliveConnection == null){
                            if (reconnection){
                                interruptWaitingCallsAndShutdown();
                            }
                        } else {
                            sendReconnectCall();
                        }
                    } catch (IOException e) {
                        logger.log(Level.WARNING, 
                            Thread.currentThread().getName() + " got exception:" + e.getMessage(), e);
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
        onDisconnect(oldConnection);
        final BlockingQueue<Call> temp = new LinkedBlockingQueue<Call>();
        queue.drainTo(temp);
        temp.add(call);
        reconnectionCalls = new ArrayList<Call>(client.getListenerManager().getListenerCalls());
        queue.addAll(reconnectionCalls);
        temp.drainTo(queue);
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

    void sendReconnectCall() {
        interruptWaitingCalls();
        enQueue(RECONNECT_CALL);
    }
}
