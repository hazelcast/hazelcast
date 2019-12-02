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
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;

/**
 * ManagementCenterService is responsible for sending statistics data to the Management Center.
 */
public class ManagementCenterService {
    public static final String SERVICE_NAME = "hz:core:managementCenterService";

    private static final long DEFAULT_UPDATE_INTERVAL = 3000;
    private static final int EVENT_QUEUE_CAPACITY = 1000;
    private static final int EXECUTOR_QUEUE_CAPACITY_PER_THREAD = 1000;

    private final HazelcastInstanceImpl instance;
    private final PrepareStateThread prepareStateThread;
    private final ILogger logger;

    private final ConsoleCommandHandler commandHandler;
    private final ClientBwListConfigHandler bwListConfigHandler;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final TimedMemberStateFactory timedMemberStateFactory;
    private final AtomicReference<String> timedMemberStateJson = new AtomicReference<>();

    private final BlockingQueue<Event> mcEvents;

    private volatile ManagementCenterEventListener eventListener;
    private volatile String lastMCConfigETag;

    public ManagementCenterService(HazelcastInstanceImpl instance) {
        this.instance = instance;
        this.logger = instance.node.getLogger(ManagementCenterService.class);
        this.commandHandler = new ConsoleCommandHandler(instance);
        this.bwListConfigHandler = new ClientBwListConfigHandler(instance.node.clientEngine);
        this.prepareStateThread = new PrepareStateThread();
        this.timedMemberStateFactory = instance.node.getNodeExtension().createTimedMemberStateFactory(instance);
        registerExecutor();

        // TODO
        mcEvents = new LinkedBlockingQueue<>(EVENT_QUEUE_CAPACITY);
        start();
    }

    private void start() {
        if (!isRunning.compareAndSet(false, true)) {
            // it is already started
            return;
        }

        timedMemberStateFactory.init();

        prepareStateThread.start();
        logger.info("Hazelcast is ready for communication with Hazelcast Management Center");
    }

    public void shutdown() {
        if (!isRunning.compareAndSet(true, false)) {
            //it is already shutdown.
            return;
        }

        logger.info("Shutting down Hazelcast Management Center Service");
        try {
            interruptThread(prepareStateThread);
        } catch (Throwable ignored) {
            ignore(ignored);
        }
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
        return Optional.ofNullable(timedMemberStateJson.get());
    }

    private void interruptThread(Thread thread) {
        if (thread != null) {
            thread.interrupt();
        }
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
        if (isRunning()) {
            mcEvents.offer(event);
            if (eventListener != null) {
                eventListener.onEventLogged(event);
            }
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

    private boolean isRunning() {
        return isRunning.get();
    }

    private final class PrepareStateThread extends Thread {

        private final long updateIntervalMs;

        private PrepareStateThread() {
            super(createThreadName(instance.getName(), "MC.State.Sender"));
            updateIntervalMs = calcUpdateInterval();
        }

        private long calcUpdateInterval() {
            long updateInterval = 3000; // managementCenterConfig.getUpdateInterval();
            return (updateInterval > 0) ? TimeUnit.SECONDS.toMillis(updateInterval) : DEFAULT_UPDATE_INTERVAL;
        }

        @Override
        public void run() {
            try {
                while (isRunning()) {
                    try {
                        TimedMemberState tms = timedMemberStateFactory.createTimedMemberState();
                        JsonObject tmsJson = new JsonObject();
                        tmsJson.add("timedMemberState", tms.toJson());
                        timedMemberStateJson.set(tmsJson.toString());
                    } catch (Throwable e) {
                        if (!(e instanceof RetryableException)) {
                            throw rethrow(e);
                        }
                        logger.warning("Can't create TimedMemberState. Will retry after " + updateIntervalMs + " ms");
                    }
                    sleep();
                }
            } catch (Throwable throwable) {
                inspectOutOfMemoryError(throwable);
                if (!(throwable instanceof InterruptedException)) {
                    logger.warning("Hazelcast Management Center Service will be shutdown due to exception.", throwable);
                    shutdown();
                }
            }
        }

        private void sleep() throws InterruptedException {
            Thread.sleep(updateIntervalMs);
        }
    }
}
