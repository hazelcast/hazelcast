/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management;

import com.hazelcast.cluster.Address;
import com.hazelcast.console.ConsoleApp;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.management.events.Event;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.executor.ExecutorType;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.ClusterProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;

/**
 * ManagementCenterService is responsible for sending statistics data to the Management Center.
 */
public class ManagementCenterService {

    static class MCEventStore {
        static final long MC_EVENTS_WINDOW_MILLIS = TimeUnit.SECONDS.toMillis(30);
        private final LongSupplier clock;
        private volatile long lastMCEventsPollMillis;
        private final ConcurrentMap<Address, Long> lastAccessTimestamps = new ConcurrentHashMap<>();
        private final BlockingQueue<Event> mcEvents;

        MCEventStore(LongSupplier clock, BlockingQueue<Event> mcEvents) {
            this.clock = clock;
            this.lastMCEventsPollMillis = clock.getAsLong();
            this.mcEvents = mcEvents;
        }

        void log(Event event) {
            if (clock.getAsLong() - lastMCEventsPollMillis > MC_EVENTS_WINDOW_MILLIS) {
                // ignore event and clear the queue if the last poll happened a while ago
                onMCEventWindowExceeded();
            } else {
                mcEvents.offer(event);
            }

        }

        void onMCEventWindowExceeded() {
            mcEvents.clear();
            lastAccessTimestamps.clear();
        }

        /**
         * @param mcRemoteAddr the address of the calling MC instance.
         * @return the events which were added to the queue since this MC last polled, or all known events if the MC polls for the
         * first time.
         */
        public List<Event> pollMCEvents(Address mcRemoteAddr) {
            Long lastAccessObj = lastAccessTimestamps.get(mcRemoteAddr);
            if (lastAccessObj == null) {
                ArrayList<Event> copy = new ArrayList<>(mcEvents);
                updateLatestAccessStats(mcRemoteAddr);
                return copy;
            } else {
                long lastAccess = lastAccessObj;
                List<Event> recentEvents = new ArrayList<>();
                for (Event evt : mcEvents) {
                    if (evt.getTimestamp() >= lastAccess) {
                        recentEvents.add(evt);
                    }
                }
                updateLatestAccessStats(mcRemoteAddr);
                return recentEvents;
            }
        }

        /**
         * Updates {@link #lastMCEventsPollMillis} to the current time, removes old entries from {@link #lastAccessTimestamps}
         * and removes the entries of {@link #mcEvents} that are already read by all known MCs.
         *
         * @param mcRemoteAddr
         */
        private void updateLatestAccessStats(Address mcRemoteAddr) {
            lastMCEventsPollMillis = clock.getAsLong();
            lastAccessTimestamps.put(mcRemoteAddr, lastMCEventsPollMillis);
            if (mcEvents.isEmpty()) {
                return;
            }
            OptionalLong maybeOldestAccess = lastAccessTimestamps.values().stream().mapToLong(Long::longValue).min();
            if (maybeOldestAccess.isPresent()) {
                long oldestAccess = maybeOldestAccess.getAsLong();
                cleanUpLastAccessTimestamps(oldestAccess);
                Iterator<Event> it = mcEvents.iterator();
                while (it.hasNext()) {
                    Event evt = it.next();
                    if (evt.getTimestamp() >= oldestAccess) {
                        break;
                    }
                    it.remove();
                }
            }
        }

        /**
         * Removes the entries from {@link #lastAccessTimestamps} which record accesses older than
         * {@link #MC_EVENTS_WINDOW_MILLIS}.
         *
         * @param oldestAccess
         */
        private void cleanUpLastAccessTimestamps(long oldestAccess) {
            if (lastMCEventsPollMillis - oldestAccess > MC_EVENTS_WINDOW_MILLIS) {
                lastAccessTimestamps.entrySet().removeIf(
                        entry -> lastMCEventsPollMillis - entry.getValue() > MC_EVENTS_WINDOW_MILLIS);
            }
        }
    }

    public static final String SERVICE_NAME = "hz:core:managementCenterService";

    private static final int MIN_EVENT_QUEUE_CAPACITY = 1000;
    private static final int EXECUTOR_QUEUE_CAPACITY_PER_THREAD = 1000;
    private static final long TMS_CACHE_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final HazelcastInstanceImpl instance;
    private final ILogger logger;

    private final AtomicReference<String> tmsJson = new AtomicReference<>();
    private final TimedMemberStateFactory tmsFactory;
    private final AtomicBoolean tmsFactoryInitialized = new AtomicBoolean(false);
    private final ConsoleCommandHandler commandHandler;
    private final ClientBwListConfigHandler bwListConfigHandler;
    private final MCEventStore eventStore;

    private volatile ManagementCenterEventListener eventListener;
    private volatile String lastMCConfigETag;
    private volatile long lastTMSUpdateNanos;

    public ManagementCenterService(HazelcastInstanceImpl instance) {
        this(instance, Clock::currentTimeMillis);
    }

    public ManagementCenterService(HazelcastInstanceImpl instance, LongSupplier clock) {
        this.instance = instance;
        this.logger = instance.node.getLogger(ManagementCenterService.class);
        this.tmsFactory = instance.node.getNodeExtension().createTimedMemberStateFactory(instance);
        int partitionCount = instance.node.getPartitionService().getPartitionCount();
        this.commandHandler = new ConsoleCommandHandler(instance);
        this.bwListConfigHandler = new ClientBwListConfigHandler(instance.node.clientEngine);
        this.eventStore = new MCEventStore(clock, new LinkedBlockingQueue<>(Math.max(MIN_EVENT_QUEUE_CAPACITY, partitionCount)));
        registerExecutor();
    }

    private void registerExecutor() {
        final ExecutionService executionService = instance.node.nodeEngine.getExecutionService();
        int threadCount = instance.node.getProperties().getInteger(ClusterProperty.MC_EXECUTOR_THREAD_COUNT);
        logger.finest("Creating new executor for Management Center service tasks with threadCount=" + threadCount);
        executionService.register(ExecutionService.MC_EXECUTOR,
                threadCount, threadCount * EXECUTOR_QUEUE_CAPACITY_PER_THREAD,
                ExecutorType.CACHED);
    }

    /**
     * Returns JSON string representation of the latest {@link TimedMemberState}.
     * The value is lazily calculated and cached for 1 second.
     *
     * @return optional JSON string (result of {@link TimedMemberState#toJson()})
     */
    @Nonnull
    public Optional<String> getTimedMemberStateJson() {
        if (tmsFactoryInitialized.compareAndSet(false, true)) {
            tmsFactory.init();
        }

        if (System.nanoTime() - lastTMSUpdateNanos <= TMS_CACHE_TIMEOUT_NANOS) {
            return Optional.ofNullable(tmsJson.get());
        }

        try {
            TimedMemberState tms;
            synchronized (tmsFactory) {
                tms = tmsFactory.createTimedMemberState();
                lastTMSUpdateNanos = System.nanoTime();
            }
            JsonObject json = new JsonObject();
            json.add("timedMemberState", tms.toJson());
            tmsJson.set(json.toString());
        } catch (Throwable e) {
            if (e instanceof RetryableException) {
                logger.warning("Failed to create TimedMemberState. Will try again on next request "
                        + "from Management Center.");
            } else {
                inspectOutOfMemoryError(e);
            }
        }
        return Optional.ofNullable(tmsJson.get());
    }

    /**
     * Logs an event to Management Center and calls the configured
     * {@link ManagementCenterEventListener} with the logged event if it is set.
     * <p>
     * Events are used by Management Center to show the user what happens when on a cluster member.
     */
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void log(Event event) {
        eventStore.log(event);
        if (eventListener != null) {
            eventListener.onEventLogged(event);
        }
    }

    // visible for tests
    void onMCEventWindowExceeded() {
        eventStore.onMCEventWindowExceeded();
    }

    // used for tests
    @SuppressWarnings("unused")
    public void setEventListener(ManagementCenterEventListener eventListener) {
        this.eventListener = eventListener;
    }

    /**
     * Polls pending MC events.
     *
     * @return polled events
     */
    @Nonnull
    public List<Event> pollMCEvents(Address mcRemoteAddr) {
        return eventStore.pollMCEvents(mcRemoteAddr);
    }

    // visible for testing
    void clear() {
        eventStore.onMCEventWindowExceeded();
    }

    /**
     * Run console command with internal {@link ConsoleCommandHandler}.
     *
     * @param command command string (see {@link ConsoleApp})
     * @return command output
     */
    public String runConsoleCommand(String command)
            throws InterruptedException {
        return commandHandler.handleCommand(command);
    }

    /**
     * Returns ETag value of last applied MC config (client B/W list filtering).
     *
     * @return last or <code>null</code>
     */
    public String getLastMCConfigETag() {
        return lastMCConfigETag;
    }

    /**
     * Applies given MC config (client B/W list filtering).
     *
     * @param eTag         ETag of new config
     * @param bwListConfig new config
     */
    public void applyMCConfig(String eTag, ClientBwListDTO bwListConfig) {
        if (eTag.equals(lastMCConfigETag)) {
            logger.warning("Client B/W list filtering config with the same ETag is already applied.");
            return;
        }

        try {
            bwListConfigHandler.applyConfig(bwListConfig);
            lastMCConfigETag = eTag;
        } catch (Exception e) {
            logger.warning("Could not apply client B/W list filtering config.", e);
            throw new HazelcastException("Error while applying MC config", e);
        }
    }
}
