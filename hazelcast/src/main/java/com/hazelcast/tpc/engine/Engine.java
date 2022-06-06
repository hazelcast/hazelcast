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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.tpc.engine.Engine.State.RUNNING;
import static com.hazelcast.tpc.engine.Engine.State.SHUTDOWN;
import static com.hazelcast.tpc.engine.Engine.State.TERMINATED;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The Engine is effectively an array of eventloops
 *
 * The Engine is not aware of any specific applications. E.g. it could execute operations, but it
 * can equally well run client requests or completely different applications.
 */
public final class Engine {

    private final boolean monitorSilent;
    private final Eventloop.Type eventloopType;
    private final int eventloopCount;
    private final Eventloop[] eventloops;
    private final MonitorThread monitorThread;
    private final AtomicReference<State> state = new AtomicReference<>(State.NEW);

    public Engine(Configuration configuration) {
        this.eventloopCount = configuration.eventloopCount;
        this.eventloopType = configuration.eventloopType;
        this.monitorSilent = configuration.monitorSilent;
        this.eventloops = new Eventloop[eventloopCount];
        this.monitorThread = new MonitorThread(eventloops, monitorSilent);

        for (int idx = 0; idx < eventloopCount; idx++) {
            switch (eventloopType) {
                case NIO:
                    NioConfiguration nioConfiguration = new NioConfiguration();
                    nioConfiguration.setThreadAffinity(configuration.threadAffinity);
                    nioConfiguration.setThreadName("eventloop-" + idx);
                    configuration.eventloopConfigUpdater.accept(nioConfiguration);
                    eventloops[idx] = new NioEventloop(nioConfiguration);
                    break;
                case EPOLL:
                    EpollConfiguration epollConfiguration = new EpollConfiguration();
                    epollConfiguration.setThreadAffinity(configuration.threadAffinity);
                    epollConfiguration.setThreadName("eventloop-" + idx);
                    configuration.eventloopConfigUpdater.accept(epollConfiguration);
                    eventloops[idx] = new EpollEventloop(epollConfiguration);
                    break;
                case IOURING:
                    IOUringConfiguration ioUringConfiguration = new IOUringConfiguration();
                    ioUringConfiguration.setThreadName("eventloop-" + idx);
                    ioUringConfiguration.setThreadAffinity(configuration.threadAffinity);
                    configuration.eventloopConfigUpdater.accept(ioUringConfiguration);
                    eventloops[idx] = new IOUringEventloop(ioUringConfiguration);
                    break;
                default:
                    throw new IllegalStateException("Unknown eventloopType:" + eventloopType);
            }
        }
    }

    /**
     * Returns the Engine State.
     *
     * This method is thread-safe.
     *
     * @return the engine state.
     */
    public State state() {
        return state.get();
    }

    public Eventloop.Type eventloopType() {
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

    public void run(int eventloopIdx, Eventloop.EventloopTask task) {
        eventloops[eventloopIdx].execute(task);
    }

    public void start() {
        for (; ; ) {
            State oldState = state.get();
            if (oldState != RUNNING) {
                throw new IllegalStateException();
            }

            if (!state.compareAndSet(oldState, RUNNING)) {
                continue;
            }

            for (Eventloop eventloop : eventloops) {
                eventloop.start();
            }

            monitorThread.start();
            return;
        }
    }

    public void shutdown() {
        for (; ; ) {
            State oldState = state.get();
            switch (oldState) {
                case NEW:
                    if(!state.compareAndSet(oldState, TERMINATED)){
                        continue;
                    }
                    break;
                case RUNNING:
                    if(!state.compareAndSet(oldState, SHUTDOWN)){
                        continue;
                    }
                    break;
                case SHUTDOWN:
                    return;
                case TERMINATED:
                    return;
                default:
                    throw new IllegalStateException();
            }

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
            return;
        }
    }

    /**
     * Contains the configuration of the {@link Engine}.
     */
    public static class Configuration {
        private int eventloopCount = Integer.parseInt(getProperty("reactor.count", "" + Runtime.getRuntime().availableProcessors()));
        private Eventloop.Type eventloopType = Eventloop.Type.fromString(getProperty("reactor.type", "nio"));
        private ThreadAffinity threadAffinity = ThreadAffinity.newSystemThreadAffinity("reactor.cpu-affinity");
        private boolean monitorSilent = Boolean.parseBoolean(getProperty("reactor.monitor.silent", "false"));
        private Consumer<Eventloop.Configuration> eventloopConfigUpdater = configuration -> {
        };

        public void setEventloopType(Eventloop.Type eventloopType) {
            this.eventloopType = checkNotNull(eventloopType, "eventloopType can't be null");
        }

        public void setEventloopConfigUpdater(Consumer<Eventloop.Configuration> eventloopConfigUpdater) {
            this.eventloopConfigUpdater = eventloopConfigUpdater;
        }

        public void setEventloopCount(int eventloopCount) {
            this.eventloopCount = checkPositive("reactorCount", eventloopCount);
        }

        public void setMonitorSilent(boolean monitorSilent) {
            this.monitorSilent = monitorSilent;
        }
    }

    public enum State {
        NEW,
        RUNNING,
        SHUTDOWN,
        TERMINATED
    }
}
