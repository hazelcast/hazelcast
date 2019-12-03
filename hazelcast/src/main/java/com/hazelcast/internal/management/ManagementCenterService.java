/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

    private static final int EVENT_QUEUE_CAPACITY = 1000;
    private static final int EXECUTOR_QUEUE_CAPACITY_PER_THREAD = 1000;
    private static final long TMS_MEMOIZATION_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final HazelcastInstanceImpl instance;
    private final ILogger logger;

    private final ConsoleCommandHandler commandHandler;
    private final ClientBwListConfigHandler bwListConfigHandler;
    private final BlockingQueue<Event> mcEvents;
    private final AtomicReference<String> timedMemberStateJson = new AtomicReference<>();
    private final TimedMemberStateFactory tmsFactory;
    private final AtomicBoolean tmsFactoryInitialized = new AtomicBoolean(false);

    private volatile ManagementCenterEventListener eventListener;
    private volatile String lastMCConfigETag;
    private volatile long lastTMSUpdateAttemptNanos = 0;

    public ManagementCenterService(HazelcastInstanceImpl instance) {
        this.instance = instance;
        this.logger = instance.node.getLogger(ManagementCenterService.class);
        this.commandHandler = new ConsoleCommandHandler(instance);
        this.bwListConfigHandler = new ClientBwListConfigHandler(instance.node.clientEngine);
        this.tmsFactory = instance.node.getNodeExtension().createTimedMemberStateFactory(instance);
        this.mcEvents = new LinkedBlockingQueue<>(EVENT_QUEUE_CAPACITY);
        registerExecutor();
        logger.info("Hazelcast is ready for communication with Hazelcast Management Center");
    }

    private void registerExecutor() {
        final ExecutionService executionService = instance.node.nodeEngine.getExecutionService();
        int threadCount = instance.node.getProperties().getInteger(ClusterProperty.MC_EXECUTOR_THREAD_COUNT);
        logger.finest("Creating new executor for Management Center service tasks with threadCount=" + threadCount);
        executionService.register(ExecutionService.MC_EXECUTOR,
                threadCount, threadCount * EXECUTOR_QUEUE_CAPACITY_PER_THREAD,
                ExecutorType.CACHED);
    }

    public Optional<String> getTimedMemberStateJson() {
        if (tmsFactoryInitialized.compareAndSet(false, true)) {
            tmsFactory.init();
        }

        if (System.nanoTime() - lastTMSUpdateAttemptNanos > TMS_MEMOIZATION_TIMEOUT_NANOS) {
            try {
                lastTMSUpdateAttemptNanos = System.nanoTime();
                TimedMemberState tms = tmsFactory.createTimedMemberState();
                JsonObject tmsJson = new JsonObject();
                tmsJson.add("timedMemberState", tms.toJson());
                timedMemberStateJson.set(tmsJson.toString());
            } catch (Throwable e) {
                if (e instanceof RetryableException) {
                    logger.warning("Failed to create TimedMemberState. Will try again on next request from Management Center.");
                } else {
                    inspectOutOfMemoryError(e);
                }
            }
        }

        return Optional.ofNullable(timedMemberStateJson.get());
    }

    public HazelcastInstanceImpl getHazelcastInstance() {
        return instance;
    }

    public ConsoleCommandHandler getCommandHandler() {
        return commandHandler;
    }

    // used for tests
    @SuppressWarnings("unused")
    public void setEventListener(ManagementCenterEventListener eventListener) {
        this.eventListener = eventListener;
    }

    /**
     * Logs an event to Management Center and calls the configured
     * {@link ManagementCenterEventListener} with the logged event if it
     * is set.
     * <p>
     * Events are used by Management Center to show the user what happens when on a cluster member.
     */
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void log(Event event) {
        // TODO ignore events if polls happened a while ago
        mcEvents.offer(event);
        if (eventListener != null) {
            eventListener.onEventLogged(event);
        }
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
        return polled;
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
