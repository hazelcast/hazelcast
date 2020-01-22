/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.console.ConsoleApp;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.management.events.Event;
import com.hazelcast.internal.util.executor.ExecutorType;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.ClusterProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;

/**
 * ManagementCenterService is responsible for sending statistics data to the Management Center.
 */
public class ManagementCenterService {

    public static final String SERVICE_NAME = "hz:core:managementCenterService";

    private static final int MIN_EVENT_QUEUE_CAPACITY = 1000;
    private static final int EXECUTOR_QUEUE_CAPACITY_PER_THREAD = 1000;
    private static final long TMS_CACHE_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final long MC_EVENTS_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final HazelcastInstanceImpl instance;
    private final ILogger logger;

    private final AtomicReference<String> tmsJson = new AtomicReference<>();
    private final TimedMemberStateFactory tmsFactory;
    private final AtomicBoolean tmsFactoryInitialized = new AtomicBoolean(false);
    private final BlockingQueue<Event> mcEvents;
    private final ConsoleCommandHandler commandHandler;
    private final ClientBwListConfigHandler bwListConfigHandler;

    private volatile ManagementCenterEventListener eventListener;
    private volatile String lastMCConfigETag;
    private volatile long lastTMSUpdateNanos;
    private volatile long lastMCEventsPollNanos = System.nanoTime();

    public ManagementCenterService(HazelcastInstanceImpl instance) {
        this.instance = instance;
        this.logger = instance.node.getLogger(ManagementCenterService.class);
        this.tmsFactory = instance.node.getNodeExtension().createTimedMemberStateFactory(instance);
        int partitionCount = instance.node.getPartitionService().getPartitionCount();
        this.mcEvents = new LinkedBlockingQueue<>(Math.max(MIN_EVENT_QUEUE_CAPACITY, partitionCount));
        this.commandHandler = new ConsoleCommandHandler(instance);
        this.bwListConfigHandler = new ClientBwListConfigHandler(instance.node.clientEngine);
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
        if (System.nanoTime() - lastMCEventsPollNanos > MC_EVENTS_WINDOW_NANOS) {
            // ignore event and clear the queue if the last poll happened a while ago
            onMCEventWindowExceeded();
        } else {
            mcEvents.offer(event);
        }

        if (eventListener != null) {
            eventListener.onEventLogged(event);
        }
    }

    // visible for tests
    void onMCEventWindowExceeded() {
        mcEvents.clear();
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
    public List<Event> pollMCEvents() {
        List<Event> polled = new ArrayList<>();
        mcEvents.drainTo(polled);
        lastMCEventsPollNanos = System.nanoTime();
        return polled;
    }

    /**
     * Run console command with internal {@link ConsoleCommandHandler}.
     *
     * @param command   command string (see {@link ConsoleApp})
     * @return          command output
     */
    public String runConsoleCommand(String command) throws InterruptedException {
        return commandHandler.handleCommand(command);
    }

    /**
     * Returns ETag value of last applied MC config (client B/W list filtering).
     *
     * @return  last or <code>null</code>
     */
    public String getLastMCConfigETag() {
        return lastMCConfigETag;
    }

    /**
     * Applies given MC config (client B/W list filtering).
     *
     * @param eTag          ETag of new config
     * @param bwListConfig  new config
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
