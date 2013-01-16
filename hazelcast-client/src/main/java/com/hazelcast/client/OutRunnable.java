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

package com.hazelcast.client;

import com.hazelcast.util.SimpleBoundedQueue;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public class OutRunnable extends IORunnable {
    final PacketWriter writer;

    final BlockingQueue<Call> queue = new LinkedBlockingQueue<Call>();

    final AtomicBoolean reconnection;

    private final Collection<Call> reconnectionCalls = new LinkedBlockingQueue<Call>();

    private final SimpleBoundedQueue<Call> q = new SimpleBoundedQueue<Call>(1000);

    private Connection connection = null;

    public OutRunnable(final HazelcastClient client, final Map<Long, Call> calls, final PacketWriter writer) {
        super(client, calls);
        this.writer = writer;
        this.reconnection = new AtomicBoolean(false);
    }

    protected void customRun() throws InterruptedException {
        if (reconnection.get()) {
            Thread.sleep(50L);
            return;
        }
        try {
            boolean written = false;
            if (queue.size() > 0 || q.size() > 0) {
                queue.drainTo(q, q.remainingCapacity());
                Call call = q.poll();
                while (call != null) {
                    writeCall(call);
                    written = true;
                    call = q.poll();
                }
            }
            try {
                if (written) {
                    writer.flush(connection);
                }
            } catch (IOException e) {
                clusterIsDown(connection);
            }
            Call call = queue.poll(12, TimeUnit.MILLISECONDS);
            if (call != null) {
                writeCall(call);
                try {
                    writer.flush(connection);
                } catch (IOException e) {
                    clusterIsDown(connection);
                }
            }
            if (reconnectionCalls.size() > 0) {
                checkOnReconnect(call);
            }
        } catch (Throwable e) {
            logger.log(Level.FINE, "OutRunnable [" + connection + "] got an exception:" + e.toString(), e);
        }
    }

    private void writeCall(Call call) {
        try {
            Connection oldConnection = connection;
            connection = client.getConnectionManager().getConnection();
            if (restoredConnection(oldConnection, connection)) {
                queue.offer(call);
                resubscribe(oldConnection);
                if (reconnectionCalls.size() == 0) {
                    client.getConnectionManager().notifyConnectionIsOpened();
                }
            } else if (connection != null) {
                if (call != RECONNECT_CALL) {
                    if (!call.isFireNforget()) {
                        callMap.put(call.getId(), call);
                    }
                    writer.write(connection, call.getRequest());
                    call.written = System.nanoTime();
                }
            } else {
                queue.offer(call);
                clusterIsDown(oldConnection);
                return;
            }
            if (reconnectionCalls.size() > 0) {
                checkOnReconnect(call);
            }
        } catch (Exception e) {
            queue.offer(call);
            clusterIsDown(connection);
        }
    }

    private void checkOnReconnect(Call call) {
        try {
            Object response = reconnectionCalls.contains(call) ?
                    call.getResponse(100L, TimeUnit.MILLISECONDS) :
                    null;
            if (response != null) {
                reconnectionCalls.remove(call);
            } else {
                for (final Iterator<Call> it = reconnectionCalls.iterator(); it.hasNext(); ) {
                    final Call c = it.next();
                    response = !c.hasResponse() ? c.getResponse(100L, TimeUnit.MILLISECONDS) : Boolean.TRUE;
                    if (response != null) {
                        it.remove();
                    }
                }
            }
        } catch (Throwable ignored) {
            // nothing to do
        }
        if (reconnectionCalls.size() == 0) {
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
        reconnectionCalls.clear();
    }

    private void clearCalls(final Collection<Call> calls) {
        if (calls == null) return;
        for (final Iterator<Call> it = calls.iterator(); it.hasNext(); ) {
            final Call c = it.next();
            if (c == RECONNECT_CALL) continue;
            c.setResponse(new NoMemberAvailableException());
            it.remove();
        }
    }

    void clusterIsDown(Connection oldConnection) {
        if (!running) {
            interruptWaitingCalls();
            return;
        }
        client.getConnectionManager().destroyConnection(oldConnection);
        if (reconnection.compareAndSet(false, true)) {
            client.runAsyncAndWait(new Runnable() {
                public void run() {
                    try {
                        final Connection lookForAliveConnection = client.getConnectionManager().lookForLiveConnection();
                        if (lookForAliveConnection == null) {
                            logger.log(Level.WARNING, "Could not restore the connection");
                            if (reconnection.get()) {
                                logger.log(Level.WARNING, "Reconnection: " + reconnection.get() + ". Interrupting and shutting down the client!");
                                interruptWaitingCalls();
                                client.getLifecycleService().shutdown();
                            }
                        } else {
                            if (running) {
                                enQueue(RECONNECT_CALL);
                            }
                        }
                    } catch (IOException e) {
                        logger.log(Level.WARNING,
                                Thread.currentThread().getName() + " got exception:" + e.getMessage(), e);
                    } finally {
                        reconnection.compareAndSet(true, false);
                    }
                }
            });
        }
    }

    private void resubscribe(Connection oldConnection) {
        onDisconnect(oldConnection);
        final BlockingQueue<Call> temp = new LinkedBlockingQueue<Call>();
        queue.drainTo(temp);
        temp.remove(RECONNECT_CALL);
        reconnectionCalls.addAll(client.getListenerManager().getListenerCalls());
        queue.addAll(reconnectionCalls);
        temp.drainTo(queue);
        queue.addAll(callMap.values());
    }

    public void enQueue(Call call) {
        try {
            if (!running) {
                throw new NoMemberAvailableException("Client is shutdown.");
            }
            logger.log(Level.FINEST, "From " + Thread.currentThread() + ": Enqueue: " + call);
            queue.offer(call);
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
    }

    public int getQueueSize() {
        return queue.size();
    }

    boolean sendReconnectCall(Connection connection) {
        if (running && !reconnection.get() && this.connection != connection && !queue.contains(RECONNECT_CALL)) {
            enQueue(RECONNECT_CALL);
            return true;
        }
        return false;
    }

    void write(Connection connection, Packet packet) throws IOException {
        if (running) {
            writer.write(connection, packet);
            writer.flush(connection);
        }
    }
}
