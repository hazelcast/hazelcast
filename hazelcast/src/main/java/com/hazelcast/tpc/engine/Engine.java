/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.tpc.engine;

import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.tpc.engine.epoll.EpollEventloop.EpollConfiguration;
import com.hazelcast.tpc.engine.frame.Frame;
import com.hazelcast.tpc.engine.iouring.IOUringEventloop.IOUringConfiguration;
import com.hazelcast.tpc.engine.nio.NioEventloop;
import com.hazelcast.tpc.engine.epoll.EpollEventloop;
import com.hazelcast.tpc.engine.iouring.IOUringEventloop;
import com.hazelcast.tpc.engine.nio.NioEventloop.NioConfiguration;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.lang.System.getProperty;
import static java.lang.System.out;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The Engine is effectively an array of eventloops
 *
 * The Engine is not aware of any specific applications. E.g. it could execute operations, but it
 * can equally well run client requests or completely different applications.
 */
public final class Engine {

    private final boolean monitorSilent;
    private final Supplier<Scheduler> schedulerSupplier;
    private final boolean spin;
    private final EventloopType eventloopType;
    private final ThreadAffinity threadAffinity;
    private final String eventloopBasename;
    private final int eventloopCount;
    private final Eventloop[] eventloops;
    private final MonitorThread monitorThread;

    public Engine(Configuration configuration) {
        this.schedulerSupplier = configuration.schedulerSupplier;
        this.eventloopCount = configuration.eventloopCount;
        this.spin = configuration.spin;
        this.eventloopType = configuration.eventloopType;
        this.threadAffinity = configuration.threadAffinity;
        this.monitorSilent = configuration.monitorSilent;
        this.eventloopBasename = configuration.eventloopBasename;
        this.eventloops = new Eventloop[eventloopCount];
        this.monitorThread = new MonitorThread(eventloops, monitorSilent);

        for (int idx = 0; idx < eventloopCount; idx++) {
            Eventloop eventloop;
            switch (eventloopType) {
                case NIO:
                    NioConfiguration nioConfig = new NioConfiguration();
                    nioConfig.setThreadName(eventloopBasename + idx);
                    nioConfig.setSpin(spin);
                    nioConfig.setScheduler(schedulerSupplier.get());
                    nioConfig.setThreadAffinity(threadAffinity);
                    eventloop = new NioEventloop(nioConfig);
                    break;
                case EPOLL:
                    EpollConfiguration epollConfig = new EpollConfiguration();
                    epollConfig.setThreadName(eventloopBasename + idx);
                    epollConfig.setSpin(spin);
                    epollConfig.setScheduler(schedulerSupplier.get());
                    epollConfig.setThreadAffinity(threadAffinity);
                    eventloop = new EpollEventloop(epollConfig);
                    break;
                case IOURING:
                    IOUringConfiguration ioUringConfig = new IOUringConfiguration();
                    ioUringConfig.setThreadName(eventloopBasename + idx);
                    ioUringConfig.setSpin(spin);
                    ioUringConfig.setScheduler(schedulerSupplier.get());
                    ioUringConfig.setThreadAffinity(threadAffinity);
                    eventloop = new IOUringEventloop(ioUringConfig);
                    break;
                default:
                    throw new IllegalStateException("Unknown reactorType:" + eventloopType);
            }

            eventloops[idx] = eventloop;
        }
    }

    public EventloopType getEventloopType() {
        return eventloopType;
    }

    public Eventloop[] eventloops() {
        return eventloops;
    }

    public int eventloopCount() {
        return eventloopCount;
    }

    public void forEach(Consumer<Eventloop> function) {
        for (Eventloop eventloop : eventloops) {
            function.accept(eventloop);
        }
    }

    public Eventloop eventloopForHash(int hash) {
        return eventloops[hashToIndex(hash, eventloops.length)];
    }

    public Eventloop eventloop(int eventloopIdx) {
        return eventloops[eventloopIdx];
    }

    public void run(int eventloopIdx, Collection<Frame> frames) {
        eventloops[eventloopIdx].execute(frames);
    }

    public void run(int eventloopIdx, Frame frame) {
        eventloops[eventloopIdx].execute(frame);
    }

    public void run(int eventloopIdx, EventloopTask task) {
        eventloops[eventloopIdx].execute(task);
    }

    public void start() {
        for (Eventloop eventloop : eventloops) {
            eventloop.start();
        }


        this.monitorThread.start();
    }

    public void shutdown() {
        for (Eventloop eventloop : eventloops) {
            eventloop.shutdown();
        }

        try {
            for (Eventloop eventloop : eventloops) {
                eventloop.awaitTermination(5, SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        monitorThread.shutdown();
    }

    public void printConfig() {
        out.println("reactor.count:" + eventloopCount);
        out.println("reactor.spin:" + spin);
        out.println("reactor.type:" + eventloopType);
        out.println("reactor.cpu-affinity:" + threadAffinity);
    }

    /**
     * Contains the configuration of the {@link Engine}.
     */
    public static class Configuration {
        private Supplier<Scheduler> schedulerSupplier;
        private int eventloopCount = Integer.parseInt(getProperty("reactor.count", "" + Runtime.getRuntime().availableProcessors()));
        private boolean spin = Boolean.parseBoolean(getProperty("reactor.spin", "false"));
        private EventloopType eventloopType = EventloopType.fromString(getProperty("reactor.type", "nio"));
        private ThreadAffinity threadAffinity = ThreadAffinity.newSystemThreadAffinity("reactor.cpu-affinity");
        private boolean monitorSilent = Boolean.parseBoolean(getProperty("reactor.monitor.silent", "false"));
        private String eventloopBasename = "eventloop-";

        /**
         * Sets the ThreadAffinity for the reactor threads.
         *
         * @param threadAffinity the ThreadAffinity
         * @throws NullPointerException if threadAffinity is null.
         */
        public void setThreadAffinity(ThreadAffinity threadAffinity) {
            this.threadAffinity = checkNotNull(threadAffinity, "threadAffinity can't be null");
        }

        public void setEventloopBasename(String baseName) {
            this.eventloopBasename = checkNotNull(baseName, "baseName can't be null");
        }

        public void setEventloopCount(int eventloopCount) {
            this.eventloopCount = checkPositive("reactorCount", eventloopCount);
        }

        public void setSchedulerSupplier(Supplier<Scheduler> schedulerSupplier) {
            this.schedulerSupplier = checkNotNull(schedulerSupplier, "schedulerSupplier can't be null");
        }

        public void setSpin(boolean spin) {
            this.spin = spin;
        }

        public void setEventloopType(EventloopType eventloopType) {
            this.eventloopType = checkNotNull(eventloopType);
        }

        public void setMonitorSilent(boolean monitorSilent) {
            this.monitorSilent = monitorSilent;
        }
    }
}
