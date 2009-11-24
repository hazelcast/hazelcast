/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.collection.SimpleBoundedQueue;
import com.hazelcast.config.ConfigProperty;
import com.hazelcast.impl.*;
import com.hazelcast.impl.BaseManager.PacketProcessor;
import com.hazelcast.nio.Packet;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ClusterService implements Runnable, Constants {
    private final Logger logger = Logger.getLogger(ClusterService.class.getName());

    private static final long PERIODIC_CHECK_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

    private static final long MAX_IDLE_NANOS = TimeUnit.SECONDS.toNanos(ConfigProperty.MAX_NO_HEARTBEAT_SECONDS.getInteger());

    private final BlockingQueue<Packet> packetQueue = new LinkedBlockingQueue<Packet>();
    private final BlockingQueue<Processable> processableQueue = new LinkedBlockingQueue<Processable>();

    private volatile boolean running = true;

    private final ReentrantLock enqueueLock = new ReentrantLock();
    private final Condition notEmpty = enqueueLock.newCondition();

    private static final int PACKET_BULK_SIZE = 32;
    private static final int PROCESSABLE_BULK_SIZE = 32;

    private final SimpleBoundedQueue<Processable> processableBulk = new SimpleBoundedQueue<Processable>(PROCESSABLE_BULK_SIZE);
    private final SimpleBoundedQueue<Packet> packetBulk = new SimpleBoundedQueue<Packet>(PACKET_BULK_SIZE);

    private long totalProcessTime = 0;

    private long lastPeriodicCheck = 0;

    private long lastCheck = 0;

    private final BaseManager.PacketProcessor[] packetProcessors = new BaseManager.PacketProcessor[ClusterOperation.OPERATION_COUNT];

    private final Runnable[] periodicRunnables = new Runnable[4];

    private final Node node;

    public ClusterService(Node node) {
        this.node = node;
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

    public void registerPacketProcessor(ClusterOperation operation, BaseManager.PacketProcessor packetProcessor) {
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
        try {
            packetQueue.put(packet);
            enqueueLock.lock();
            notEmpty.signal();
        } catch (InterruptedException e) {
            node.handleInterruptedException(Thread.currentThread(), e);
        } finally {
            enqueueLock.unlock();
        }
    }

    public void enqueueAndReturn(Processable processable) {
        try {
            processableQueue.put(processable);
            enqueueLock.lock();
            notEmpty.signal();
        } catch (InterruptedException e) {
            node.handleInterruptedException(Thread.currentThread(), e);
        } finally {
            enqueueLock.unlock();
        }
    }

    public void processPacket(Packet packet) {
        if (!running) return;
        final long processStart = System.nanoTime();
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
        final long processEnd = System.nanoTime();
        final long elipsedTime = processEnd - processStart;
        totalProcessTime += elipsedTime;
    }

    public void processProcessable(Processable processable) {
        if (!running) return;
        final long processStart = System.nanoTime();
        processable.process();
        final long processEnd = System.nanoTime();
        final long elipsedTime = processEnd - processStart;
        totalProcessTime += elipsedTime;
    }

    public void run() {
        boolean readPackets = false;
        boolean readProcessables = false;
        while (running) {
            readPackets = (dequeuePackets() != 0);
            readProcessables = (dequeueProcessables() != 0);
            if (!readPackets && !readProcessables) {
                enqueueLock.lock();
                try {
                    notEmpty.await(100, TimeUnit.MILLISECONDS);
                    checkPeriodics();
                } catch (InterruptedException e) {
                    node.handleInterruptedException(Thread.currentThread(), e);
                } finally {
                    enqueueLock.unlock();
                }
            }
        }
        processableBulk.clear();
        packetBulk.clear();
        packetQueue.clear();
        processableQueue.clear();
    }

    private int dequeuePackets() {
        Packet packet = null;
        int retval = 0;
        try {
            packetQueue.drainTo(packetBulk, PACKET_BULK_SIZE);
            final int size = packetBulk.size();
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    packet = packetBulk.remove();
                    checkPeriodics();
                    processPacket(packet);
                }
            }
            retval = size;
        } catch (final Throwable e) {
            logger.log(Level.SEVERE, "error processing messages  packet=" + packet, e);
        }
        return retval;
    }

    private int dequeueProcessables() {
        Processable processable = null;
        int retval = 0;
        try {
            processableQueue.drainTo(processableBulk, PROCESSABLE_BULK_SIZE);
            final int size = processableBulk.size();
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    processable = processableBulk.remove();
                    checkPeriodics();
                    processProcessable(processable);
                }
            }
            retval = size;
            checkPeriodics();
        } catch (final Throwable e) {
            logger.log(Level.SEVERE, "error processing messages  obj=" + processable, e);
        }
        return retval;
    }

    public void start() {
        totalProcessTime = 0;
        lastPeriodicCheck = System.nanoTime();
        lastCheck = System.nanoTime();
        running = true;
    }

    public void stop() {
        packetQueue.clear();
        processableQueue.clear();
        try {
            final CountDownLatch l = new CountDownLatch(1);
            processableQueue.put(new Processable() {
                public void process() {
                    running = false;
                    l.countDown();
                }
            });
            l.await();
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
        final long now = System.nanoTime();
        if ((now - lastCheck) > MAX_IDLE_NANOS) {
            logger.log(Level.INFO, "Hazelcast ServiceThread is blocked. Restarting Hazelcast!");
            new Thread(new Runnable() {
                public void run() {
                    node.factory.restart();
                }
            }).start();
        }
        lastCheck = now;
        if ((now - lastPeriodicCheck) > PERIODIC_CHECK_INTERVAL_NANOS) {
            for (Runnable runnable : periodicRunnables) {
                if (runnable != null) {
                    runnable.run();
                }
            }
            lastPeriodicCheck = now;
        }
    }

    public long getTotalProcessTime() {
        return totalProcessTime;
    }
}
