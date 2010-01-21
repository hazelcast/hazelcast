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

package com.hazelcast.cluster;

import com.hazelcast.impl.*;
import com.hazelcast.impl.base.PacketProcessor;
import com.hazelcast.nio.Packet;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ClusterService implements Runnable, Constants {
    private final Logger logger = Logger.getLogger(ClusterService.class.getName());

    private final long PERIODIC_CHECK_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(1);

    private final long MAX_IDLE_MILLIS;

    private final boolean RESTART_ON_MAX_IDLE;

    private final Queue<Packet> packetQueue = new ConcurrentLinkedQueue<Packet>();
    private final Queue<Processable> processableQueue = new ConcurrentLinkedQueue<Processable>();

    private volatile boolean running = true;

    private final Object notEmptyLock = new Object();

    private static final int PACKET_BULK_SIZE = 64;
    private static final int PROCESSABLE_BULK_SIZE = 64;

    private long lastPeriodicCheck = 0;

    private long lastCheck = 0;

    private final PacketProcessor[] packetProcessors = new PacketProcessor[ClusterOperation.OPERATION_COUNT];

    private final Runnable[] periodicRunnables = new Runnable[4];

    private final Node node;

    public ClusterService(Node node) {
        this.node = node;
        MAX_IDLE_MILLIS = node.groupProperties.MAX_NO_HEARTBEAT_SECONDS.getInteger();
        RESTART_ON_MAX_IDLE = node.groupProperties.RESTART_ON_MAX_IDLE.getBoolean();
    }

    public void registerPeriodicRunnable(Runnable runnable) {
        int len = periodicRunnables.length;
        for (int i = 0; i < len; i++) {
            if (periodicRunnables[i] == null) {
                periodicRunnables[i] = runnable;
                return;
            }
        }
        throw new RuntimeException("Not enough space for a runnable " + runnable);
    }

    public void registerPacketProcessor(ClusterOperation operation, PacketProcessor packetProcessor) {
        if (packetProcessors[operation.getValue()] != null) {
            logger.log(Level.SEVERE, operation + " is registered already with "
                    + packetProcessors[operation.getValue()]);
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
        packetQueue.offer(packet);
        synchronized (notEmptyLock) {
            notEmptyLock.notify();
        }
    }

    public void enqueueAndReturn(Processable processable) {
        processableQueue.offer(processable);
        synchronized (notEmptyLock) {
            notEmptyLock.notify();
        }
    }

    public void processPacket(Packet packet) {
        if (!running) return;
        final MemberImpl memberFrom = node.clusterManager.getMember(packet.conn.getEndPoint());
        if (memberFrom != null) {
            memberFrom.didRead();
        }
        if (packet.operation.getValue() < 0 || packet.operation.getValue() >= packetProcessors.length) {
            String msg = "Unknown operation " + packet.operation;
            logger.log(Level.SEVERE, msg);
            throw new RuntimeException(msg);
        }
        PacketProcessor packetProcessor = packetProcessors[packet.operation.getValue()];
        if (packetProcessor == null) {
            String msg = "No Packet processor found for operation : " + packet.operation;
            logger.log(Level.SEVERE, msg);
            throw new RuntimeException(msg);
        }
        packetProcessor.process(packet);
    }

    public void processProcessable(Processable processable) {
        if (!running) return;
        processable.process();
    }

    public void run() {
        boolean readPackets = false;
        boolean readProcessables = false;
        while (running) {
            readPackets = (dequeuePackets() != 0);
            readProcessables = (dequeueProcessables() != 0);
            if (!readPackets && !readProcessables) {
                try {
                    synchronized (notEmptyLock) {
                        notEmptyLock.wait(100);
                    }
                    checkPeriodics();
                } catch (InterruptedException e) {
                    node.handleInterruptedException(Thread.currentThread(), e);
                }
            }
        }
        packetQueue.clear();
        processableQueue.clear();
    }

    private int dequeuePackets() {
        Packet packet = null;
        try {
            for (int i = 0; i < PACKET_BULK_SIZE; i++) {
                checkPeriodics();
                packet = packetQueue.poll();
                if (packet == null) {
                    return i;
                }
                processPacket(packet);
            }
        } catch (final Throwable e) {
            logger.log(Level.SEVERE, "error processing messages  packet=" + packet, e);
        }
        return PACKET_BULK_SIZE;
    }

    private int dequeueProcessables() {
        Processable processable = null;
        try {
            for (int i = 0; i < PROCESSABLE_BULK_SIZE; i++) {
                checkPeriodics();
                processable = processableQueue.poll();
                if (processable == null) {
                    return i;
                }
                processProcessable(processable);
            }
        } catch (final Throwable e) {
            logger.log(Level.SEVERE, "error processing messages  processable=" + processable, e);
        }
        return PACKET_BULK_SIZE;
    }

    public void start() {
        lastPeriodicCheck = System.currentTimeMillis();
        lastCheck = System.currentTimeMillis();
        running = true;
    }

    public void stop() {
        packetQueue.clear();
        processableQueue.clear();
        try {
            final CountDownLatch stopLatch = new CountDownLatch(1);
            processableQueue.offer(new Processable() {
                public void process() {
                    running = false;
                    stopLatch.countDown();
                }
            });
            stopLatch.await();
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public String toString() {
        return "ClusterService packetQueueSize=" + packetQueue.size()
                + "unknownQueueSize=" + processableQueue.size() + " master= " + node.master()
                + " master= " + node.getMasterAddress();
    }

    private void checkPeriodics() {
        final long now = System.currentTimeMillis();
        if ((now - lastCheck) > MAX_IDLE_MILLIS) {
            StringBuilder sb = new StringBuilder("Hazelcast ServiceThread is blocked for ");
            sb.append((now - lastCheck));
            sb.append(" ms. Restarting Hazelcast!");
            sb.append("\n\tnow:" + now);
            sb.append("\n\tlastCheck:" + lastCheck);
            sb.append("\n\tmaxIdleMillis:" + MAX_IDLE_MILLIS);
            sb.append("\n\tRESTART_ON_MAX_IDLE:" + RESTART_ON_MAX_IDLE);
            sb.append("\n");
            logger.log(Level.INFO, sb.toString());
            if (RESTART_ON_MAX_IDLE) {
                new Thread(new Runnable() {
                    public void run() {
                        node.factory.restart();
                    }
                }, "hz.RestartThread").start();
            }
        }
        lastCheck = now;
        if ((now - lastPeriodicCheck) > PERIODIC_CHECK_INTERVAL_MILLIS) {
            for (Runnable runnable : periodicRunnables) {
                if (runnable != null) {
                    runnable.run();
                }
            }
            lastPeriodicCheck = now;
        }
    }
}
