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

import com.hazelcast.impl.*;
import com.hazelcast.collection.SimpleBoundedQueue;
import com.hazelcast.impl.BaseManager.PacketProcessor;
import com.hazelcast.impl.BaseManager.Processable;
import com.hazelcast.nio.Packet;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ClusterService implements Runnable, Constants {
    protected final static Logger logger = Logger.getLogger(ClusterService.class.getName());

    private static final ClusterService instance = new ClusterService();

    private static final long PERIODIC_CHECK_INTERVAL = TimeUnit.SECONDS.toNanos(1);

    private static final long UTILIZATION_CHECK_INTERVAL = TimeUnit.SECONDS.toNanos(10);

    private final boolean DEBUG = Build.DEBUG;

    private final BlockingQueue queue = new LinkedBlockingQueue();

    private volatile boolean running = true;

    private static final int BULK_SIZE = 64;

    private final SimpleBoundedQueue bulk = new SimpleBoundedQueue(BULK_SIZE);

    private long start = 0;

    private long totalProcessTime = 0;

    private long lastPeriodicCheck = 0;

    private final BaseManager.PacketProcessor[] packetProcessors = new BaseManager.PacketProcessor[300];

    private final Runnable[] periodicRunnables = new Runnable[3];

    private ClusterService() {
    }

    public static ClusterService get() {
        return instance;
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

    public void enqueueAndReturn(final Object message) {
        try {
            queue.put(message);
        } catch (final InterruptedException e) {
            Node.get().handleInterruptedException(Thread.currentThread(), e);
        }
    }

    public void process(final Object obj) {
        if (!running) return;
        final long processStart = System.nanoTime();
        if (obj instanceof Packet) {
            final Packet packet = (Packet) obj;
            final MemberImpl memberFrom = ClusterManager.get().getMember(packet.conn.getEndPoint());
            if (memberFrom != null) {
                memberFrom.didRead();
            }
            if (packet.operation.getValue() < 0 || packet.operation.getValue() >= packetProcessors.length) {
                logger.log(Level.SEVERE, "Unknown operation " + packet.operation);
                return;
            }
            PacketProcessor packetProcessor = packetProcessors[packet.operation.getValue()];
            if (packetProcessor == null) {
                logger.log(Level.SEVERE, "No Packet processor found for operation : "
                        + packet.operation);
            }
            packetProcessor.process(packet);
        } else if (obj instanceof Processable) {
            ((Processable) obj).process();
        } else {
            logger.log(Level.SEVERE, "Cannot process. Unknown object: " + obj);
        }
        final long processEnd = System.nanoTime();
        final long elipsedTime = processEnd - processStart;
        totalProcessTime += elipsedTime;
//        final long duration = (processEnd - start);
//		if (duration > UTILIZATION_CHECK_INTERVAL) {
//			if (DEBUG) {
//				logger.log(Level.FINEST, "ServiceProcessUtilization: "
//						+ ((totalProcessTime * 100) / duration) + " %");
//			}
//			start = processEnd;
//			totalProcessTime = 0;
//		}
    }

    public void run() {
        while (running) {
            Object obj = null;
            try {
                queue.drainTo(bulk, BULK_SIZE);
                final int size = bulk.size();
                if (size > 0) {
                    for (int i = 0; i < size; i++) {
                        obj = bulk.remove();
                        checkPeriodics();
                        process(obj);
                    }
                } else {
                    obj = queue.poll(100, TimeUnit.MILLISECONDS);
                    checkPeriodics();
                    if (obj != null) {
                        process(obj);
                    }
                }
            } catch (final InterruptedException e) {
                Node.get().handleInterruptedException(Thread.currentThread(), e);
            } catch (final Throwable e) {
                logger.log(Level.FINEST, e + ",  message: " + e + ", obj=" + obj, e);
                e.printStackTrace();
                System.out.println(bulk + " Exception when handling " + obj);
            }
        }
        bulk.clear();
        queue.clear();
    }

    public void start() {
        totalProcessTime = 0;
        lastPeriodicCheck = 0;
        start = System.nanoTime();
        running = true;
    }

    public void stop() {
        queue.clear();
        running = false;
    }

    @Override
    public String toString() {
        return "ClusterService queueSize=" + queue.size() + " master= " + Node.get().master()
                + " master= " + Node.get().getMasterAddress();
    }

    private void checkPeriodics() {
        final long now = System.nanoTime();
        if ((now - lastPeriodicCheck) > PERIODIC_CHECK_INTERVAL) {
//            ClusterManager.get().heartBeater();
//            ClusterManager.get().checkScheduledActions();
            for (Runnable runnable : periodicRunnables) {
                if (runnable != null) {
                    runnable.run();
                }
            }
            lastPeriodicCheck = now;
        }
    }

}
