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

package com.hazelcast.cluster;

import com.hazelcast.impl.*;
import com.hazelcast.impl.base.PacketProcessor;
import com.hazelcast.impl.base.SystemLogService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ThreadWatcher;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;

import static com.hazelcast.impl.base.SystemLogService.Level.INFO;

public final class ClusterService implements Runnable, Constants {

    private static final int PACKET_BULK_SIZE = 64;

    private static final int PROCESSABLE_BULK_SIZE = 64;

    private final ILogger logger;

    private final long MAX_IDLE_MILLIS;

    private final boolean RESTART_ON_MAX_IDLE;

    private final Queue<Packet> packetQueue = new ConcurrentLinkedQueue<Packet>();

    private final Queue<Processable> processableQueue = new ConcurrentLinkedQueue<Processable>();

    private final Queue<Processable> processablePriorityQueue = new ConcurrentLinkedQueue<Processable>();

    private final PacketProcessor[] packetProcessors = new PacketProcessor[ClusterOperation.LENGTH];

    private final Node node;

    private long lastIdleCheck = 0;

    private volatile boolean running = true;

    private final ThreadWatcher threadWatcher = new ThreadWatcher();

    private final Thread serviceThread;

    public ClusterService(Node node) {
        this.node = node;
        this.logger = node.getLogger(ClusterService.class.getName());
        MAX_IDLE_MILLIS = node.groupProperties.MAX_NO_HEARTBEAT_SECONDS.getInteger() * 1000L;
        RESTART_ON_MAX_IDLE = node.groupProperties.RESTART_ON_MAX_IDLE.getBoolean();
        serviceThread = new Thread(node.threadGroup, this, node.getThreadNamePrefix("ServiceThread"));
    }

    public Thread getServiceThread() {
        return serviceThread;
    }

    public void registerPacketProcessor(ClusterOperation operation, PacketProcessor packetProcessor) {
        PacketProcessor processor = packetProcessors[operation.getValue()];
        if (processor != null) {
            logger.log(Level.SEVERE, operation + " is registered already with " + processor);
        }
        packetProcessors[operation.getValue()] = packetProcessor;
    }

    public PacketProcessor getPacketProcessor(ClusterOperation operation) {
        PacketProcessor packetProcessor = packetProcessors[operation.getValue()];
        if (packetProcessor == null) {
            logger.log(Level.SEVERE, operation + " has no registered processor!");
        }
        return packetProcessor;
    }

    public void enqueuePacket(Packet packet) {
        if (packet.callId != -1) {
            SystemLogService css = node.getSystemLogService();
            packet.callState = css.getOrCreateCallState(packet.callId, packet.lockAddress, packet.threadId);
            if (css.shouldLog(INFO)) {
                css.info(packet, "Enqueue Packet ", packet.operation);
            }
        }
        packetQueue.offer(packet);
        unpark();
    }

    public boolean enqueueAndWait(final Processable processable, final int seconds) {
        try {
            final CountDownLatch l = new CountDownLatch(1);
            enqueueAndReturn(new Processable() {
                public void process() {
                    processable.process();
                    l.countDown();
                }
            });
            node.checkNodeState();
            return l.await(seconds, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
        return false;
    }

    public void enqueueAndWait(final Processable processable) {
        try {
            final CountDownLatch l = new CountDownLatch(1);
            enqueueAndReturn(new Processable() {
                public void process() {
                    processable.process();
                    l.countDown();
                }
            });
            node.checkNodeState();
            l.await();
        } catch (InterruptedException ignored) {
        }
    }

    public void enqueueAndReturn(Processable processable) {
        processableQueue.offer(processable);
        unpark();
    }

    public void enqueuePriorityAndReturn(Processable processable) {
        processablePriorityQueue.offer(processable);
        unpark();
    }

    private void unpark() {
        LockSupport.unpark(serviceThread);
    }

    private void processPacket(Packet packet) {
        if (!running) return;
        final Address endPoint = packet.conn.getEndPoint();
        final MemberImpl memberFrom = node.clusterManager.getMember(endPoint);
        if (memberFrom != null) {
            memberFrom.didRead();
        }
        if (packet.operation.getValue() < 0 || packet.operation.getValue() > ClusterOperation.LENGTH) {
            String msg = "Unknown operation " + packet.operation;
            logger.log(Level.SEVERE, msg);
            throw new RuntimeException(msg);
        }
        PacketProcessor packetProcessor = packetProcessors[packet.operation.getValue()];
        if (packetProcessor == null) {
            String msg = "No Packet processor found for operation : " + packet.operation + " from " + packet.conn;
            logger.log(Level.SEVERE, msg);
            throw new RuntimeException(msg);
        }
        SystemLogService css = node.getSystemLogService();
        if (css.shouldLog(INFO)) {
            css.logObject(packet, INFO, "Processing packet");
            css.logObject(packet, INFO, packetProcessor.getClass());
        }
        packetProcessor.process(packet);
    }

    private void processProcessable(Processable processable) {
        if (!running) return;
        processable.process();
    }

    public void run() {
        ThreadContext.get().setCurrentFactory(node.factory);
        boolean readPackets = false;
        boolean readProcessables = false;
        while (running) {
            try {
                threadWatcher.incrementRunCount();
                readPackets = (dequeuePackets() != 0);
                readProcessables = (dequeueProcessables() != 0);
                if (!readPackets && !readProcessables) {
                    try {
                        long startWait = System.nanoTime();
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(473)); // magic number :)
                        long now = System.nanoTime();
                        threadWatcher.addWait((now - startWait), now);
                    } catch (Exception e) {
                        node.handleInterruptedException(Thread.currentThread(), e);
                    }
                }
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            } catch (Throwable e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        packetQueue.clear();
        processableQueue.clear();
    }

    private void publishUtilization() {
        node.getCpuUtilization().serviceThread = threadWatcher.publish(running);
    }

    private int dequeuePackets() throws Throwable {
        Packet packet = null;
        try {
            for (int i = 0; i < PACKET_BULK_SIZE; i++) {
                dequeuePriorityProcessables();
                packet = packetQueue.poll();
                if (packet == null) {
                    return i;
                }
                processPacket(packet);
            }
        } catch (OutOfMemoryError e) {
            throw e;
        } catch (Throwable e) {
            logger.log(Level.SEVERE, "error processing messages  packet=" + packet, e);
            throw e;
        }
        return PACKET_BULK_SIZE;
    }

    private int dequeueProcessables() throws Throwable {
        Processable processable = null;
        try {
            for (int i = 0; i < PROCESSABLE_BULK_SIZE; i++) {
                dequeuePriorityProcessables();
                processable = processableQueue.poll();
                if (processable == null) {
                    return i;
                }
                processProcessable(processable);
            }
        } catch (OutOfMemoryError e) {
            throw e;
        } catch (Throwable e) {
            logger.log(Level.SEVERE, "error processing messages  processable=" + processable, e);
            throw e;
        }
        return PACKET_BULK_SIZE;
    }

    private int dequeuePriorityProcessables() throws Throwable {
        Processable processable = processablePriorityQueue.poll();
        try {
            while (processable != null) {
                processProcessable(processable);
                processable = processablePriorityQueue.poll();
            }
        } catch (OutOfMemoryError e) {
            throw e;
        } catch (Throwable e) {
            logger.log(Level.SEVERE, "error processing messages processable=" + processable, e);
            throw e;
        }
        return PACKET_BULK_SIZE;
    }

    public void start() {
        lastIdleCheck = Clock.currentTimeMillis();
        running = true;
    }

    final CountDownLatch stopLatch = new CountDownLatch(1);
    final Processable stopProcessable = new Processable() {
        public void process() {
            node.cleanupServiceThread();
            running = false;
            stopLatch.countDown();
        }
    };

    public void stop() {
        packetQueue.clear();
        processableQueue.clear();
        try {
            processableQueue.offer(stopProcessable);
            stopLatch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public String toString() {
        return "ClusterService packetQueueSize=" + packetQueue.size()
                + "processableQueueSize=" + processableQueue.size() + " isMaster= " + node.isMaster()
                + " master= " + node.getMasterAddress();
    }

    public void checkIdle() {
        final long now = Clock.currentTimeMillis();
        if (RESTART_ON_MAX_IDLE && (now - lastIdleCheck) > MAX_IDLE_MILLIS) {
            if (logger.isLoggable(Level.INFO)) {
                final StringBuilder sb = new StringBuilder("Hazelcast ServiceThread is blocked for ");
                sb.append((now - lastIdleCheck));
                sb.append(" ms. Restarting Hazelcast!");
                sb.append("\n\tnow:").append(now);
                sb.append("\n\tlastIdleCheck:").append(lastIdleCheck);
                sb.append("\n\tmaxIdleMillis:").append(MAX_IDLE_MILLIS);
                sb.append("\n\tRESTART_ON_MAX_IDLE:").append(RESTART_ON_MAX_IDLE);
                sb.append("\n");
                logger.log(Level.INFO, sb.toString());
            }
            new Thread(new Runnable() {
                public void run() {
                    node.factory.restart();
                }
            }, "hz.RestartThread").start();
        }
        lastIdleCheck = now;
        publishUtilization();
    }
}
