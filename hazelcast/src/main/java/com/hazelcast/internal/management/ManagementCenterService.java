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

import com.hazelcast.cache.impl.JCacheDetector;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberAttributeEvent;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.ascii.rest.HttpCommand;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.management.events.Event;
import com.hazelcast.internal.management.events.EventBatch;
import com.hazelcast.internal.management.operation.UpdateManagementCenterUrlOperation;
import com.hazelcast.internal.management.request.AsyncConsoleRequest;
import com.hazelcast.internal.management.request.ChangeClusterStateRequest;
import com.hazelcast.internal.management.request.ChangeWanStateRequest;
import com.hazelcast.internal.management.request.ClearWanQueuesRequest;
import com.hazelcast.internal.management.request.ClusterPropsRequest;
import com.hazelcast.internal.management.request.ConsoleCommandRequest;
import com.hazelcast.internal.management.request.ConsoleRequest;
import com.hazelcast.internal.management.request.ExecuteScriptRequest;
import com.hazelcast.internal.management.request.ForceStartNodeRequest;
import com.hazelcast.internal.management.request.GetCacheEntryRequest;
import com.hazelcast.internal.management.request.GetClusterStateRequest;
import com.hazelcast.internal.management.request.GetMapEntryRequest;
import com.hazelcast.internal.management.request.GetMemberSystemPropertiesRequest;
import com.hazelcast.internal.management.request.MapConfigRequest;
import com.hazelcast.internal.management.request.MemberConfigRequest;
import com.hazelcast.internal.management.request.PromoteMemberRequest;
import com.hazelcast.internal.management.request.RunGcRequest;
import com.hazelcast.internal.management.request.ShutdownClusterRequest;
import com.hazelcast.internal.management.request.ThreadDumpRequest;
import com.hazelcast.internal.management.request.TriggerPartialStartRequest;
import com.hazelcast.internal.management.request.WanCheckConsistencyRequest;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getObject;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.ASYNC_EXECUTOR;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * ManagementCenterService is responsible for sending statistics data to the Management Center.
 */
public class ManagementCenterService {
    public static final String SERVICE_NAME = "hz:core:managementCenterService";

    private static final int HTTP_SUCCESS = 200;
    private static final int HTTP_NOT_MODIFIED = 304;
    private static final int CONNECTION_TIMEOUT_MILLIS = 5000;
    private static final long SLEEP_BETWEEN_POLL_MILLIS = 1000;
    private static final long DEFAULT_UPDATE_INTERVAL = 3000;
    private static final long EVENT_SEND_INTERVAL_MILLIS = 1000;

    private final HazelcastInstanceImpl instance;
    private final TaskPollThread taskPollThread;
    private final StateSendThread stateSendThread;
    private final PrepareStateThread prepareStateThread;
    private final EventSendThread eventSendThread;
    private final ILogger logger;

    private final ConsoleCommandHandler commandHandler;
    private final ClientBwListConfigHandler bwListConfigHandler;
    private final ManagementCenterConfig managementCenterConfig;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final TimedMemberStateFactory timedMemberStateFactory;
    private final ManagementCenterConnectionFactory connectionFactory;
    private final AtomicReference<String> timedMemberStateJson = new AtomicReference<>();
    private final BlockingQueue<Event> events = new LinkedBlockingQueue<>();

    private volatile String managementCenterUrl;
    private volatile boolean urlChanged;
    private volatile boolean manCenterConnectionLost;
    private volatile boolean taskPollFailed;
    private volatile boolean eventSendFailed;
    private volatile ManagementCenterEventListener eventListener;
    private volatile String lastMCConfigETag;

    public ManagementCenterService(HazelcastInstanceImpl instance) {
        this.instance = instance;
        this.logger = instance.node.getLogger(ManagementCenterService.class);
        this.managementCenterConfig = getManagementCenterConfig();
        this.managementCenterUrl = getManagementCenterUrl();
        this.commandHandler = new ConsoleCommandHandler(instance);
        this.bwListConfigHandler = new ClientBwListConfigHandler(instance.node.clientEngine);
        this.taskPollThread = new TaskPollThread();
        this.stateSendThread = new StateSendThread();
        this.prepareStateThread = new PrepareStateThread();
        this.eventSendThread = new EventSendThread();
        this.timedMemberStateFactory = instance.node.getNodeExtension().createTimedMemberStateFactory(instance);
        this.connectionFactory = instance.node.getNodeExtension().getManagementCenterConnectionFactory();

        if (this.managementCenterConfig.isEnabled()) {
            this.instance.getCluster().addMembershipListener(new ManagementCenterService.MemberListenerImpl());
            start();
        }
    }

    private String getManagementCenterUrl() {
        return managementCenterConfig.getUrl();
    }

    private ManagementCenterConfig getManagementCenterConfig() {
        ManagementCenterConfig config = instance.node.config.getManagementCenterConfig();
        if (config == null) {
            throw new IllegalStateException("ManagementCenterConfig can't be null!");
        }
        return config;
    }

    static String cleanupUrl(String url) {
        if (url == null) {
            return null;
        }
        return url.endsWith("/") ? url : url + '/';
    }

    private void start() {
        if (managementCenterUrl == null) {
            logger.warning("Can't start Hazelcast Management Center Service: web-server URL is null!");
            return;
        }

        if (!isRunning.compareAndSet(false, true)) {
            // it is already started
            return;
        }

        timedMemberStateFactory.init();
        try {
            if (connectionFactory != null) {
                connectionFactory.init(managementCenterConfig.getMutualAuthConfig());
            }
        } catch (Exception e) {
            throw rethrow(e);
        }

        taskPollThread.start();
        prepareStateThread.start();
        stateSendThread.start();
        eventSendThread.start();
        logger.info("Hazelcast will connect to Hazelcast Management Center on address: \n" + managementCenterUrl);
    }

    public void shutdown() {
        if (!isRunning.compareAndSet(true, false)) {
            //it is already shutdown.
            return;
        }

        logger.info("Shutting down Hazelcast Management Center Service");
        try {
            interruptThread(stateSendThread);
            interruptThread(taskPollThread);
            interruptThread(prepareStateThread);
            interruptThread(eventSendThread);
        } catch (Throwable ignored) {
            ignore(ignored);
        }
    }

    public Optional<String> getTimedMemberStateJson() {
        return Optional.ofNullable(timedMemberStateJson.get());
    }

    public byte[] clusterWideUpdateManagementCenterUrl(String newUrl) {
        try {
            final Collection<Member> memberList = instance.node.clusterService.getMembers();
            for (Member member : memberList) {
                send(member.getAddress(), new UpdateManagementCenterUrlOperation(newUrl));
            }

            return HttpCommand.RES_204;
        } catch (Throwable throwable) {
            logger.warning("New Management Center url cannot be assigned.", throwable);
            return HttpCommand.RES_500;
        }
    }

    public void updateManagementCenterUrl(String newUrl) {
        if (newUrl == null) {
            return;
        }

        if (newUrl.equals(managementCenterUrl)) {
            return;
        }
        managementCenterUrl = newUrl;

        if (!isRunning()) {
            start();
        }

        urlChanged = true;
        logger.info("Management Center URL has changed. Hazelcast will connect to Management Center on address:\n"
                + managementCenterUrl);
    }

    private void interruptThread(Thread thread) {
        if (thread != null) {
            thread.interrupt();
        }
    }

    public InvocationFuture<Object> callOnAddress(Address address, Operation operation) {
        // TODO: why are we always executing on the MapService?
        OperationService operationService = instance.node.nodeEngine.getOperationService();
        return operationService.invokeOnTarget(MapService.SERVICE_NAME, operation, address);
    }

    public InvocationFuture<Object> callOnThis(Operation operation) {
        return callOnAddress(instance.node.getThisAddress(), operation);
    }

    public JsonObject syncCallOnThis(Operation operation) {
        InvocationFuture<Object> future = callOnThis(operation);
        JsonObject result = new JsonObject();
        Object operationResult;
        try {
            operationResult = future.get();
            if (operationResult == null) {
                result.add("result", "success");
            } else {
                result.add("result", operationResult.toString());
            }
        } catch (ExecutionException e) {
            result.add("result", e.getMessage());
            result.add("stackTrace", ExceptionUtil.toString(e));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result.add("result", e.getMessage());
            result.add("stackTrace", ExceptionUtil.toString(e));
        }
        return result;
    }

    public InvocationFuture<Object> callOnMember(Member member, Operation operation) {
        return callOnAddress(member.getAddress(), operation);
    }

    public static Object resolveFuture(Future<Object> future) {
        try {
            return future.get();
        } catch (Throwable t) {
            return ExceptionUtil.toString(t);
        }
    }

    public void send(Address address, Operation operation) {
        OperationService operationService = instance.node.nodeEngine.getOperationService();
        operationService.createInvocationBuilder(MapService.SERVICE_NAME, operation, address).invoke();
    }

    public HazelcastInstanceImpl getHazelcastInstance() {
        return instance;
    }

    public ConsoleCommandHandler getCommandHandler() {
        return commandHandler;
    }

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
    public void log(Event event) {
        if (this.managementCenterConfig.isEnabled() && isRunning()) {
            events.add(event);
            if (eventListener != null) {
                eventListener.onEventLogged(event);
            }
        }
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

    private boolean post(HttpURLConnection connection) throws IOException {
        int responseCode = connection.getResponseCode();
        if (responseCode != HTTP_SUCCESS && !manCenterConnectionLost) {
            logger.warning("Failed to send response, responseCode:" + responseCode + " url:" + connection.getURL());
        }
        return responseCode == HTTP_SUCCESS;
    }

    private HttpURLConnection openJsonConnection(URL url) throws IOException {
        if (logger.isFinestEnabled()) {
            logger.finest("Opening connection to Management Center:" + url);
        }

        HttpURLConnection connection = (HttpURLConnection) (connectionFactory != null
                ? connectionFactory.openConnection(url)
                : url.openConnection());

        connection.setDoOutput(true);
        connection.setConnectTimeout(CONNECTION_TIMEOUT_MILLIS);
        connection.setReadTimeout(CONNECTION_TIMEOUT_MILLIS);
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestMethod("POST");
        return connection;
    }

    private static void sleepIfPossible(long updateIntervalMs, long elapsedMs) throws InterruptedException {
        long sleepTimeMs = updateIntervalMs - elapsedMs;
        if (sleepTimeMs > 0) {
            Thread.sleep(sleepTimeMs);
        }
    }

    /**
     * Thread for sending events to the Management Center.
     */
    private final class EventSendThread extends Thread {

        private EventSendThread() {
            super(createThreadName(instance.getName(), "MC.Event.Sender"));
        }

        @Override
        public void run() {
            try {
                while (isRunning()) {
                    long startMs = Clock.currentTimeMillis();
                    sendEvents();
                    long endMs = Clock.currentTimeMillis();
                    sleepIfPossible(EVENT_SEND_INTERVAL_MILLIS, endMs - startMs);
                }
            } catch (Throwable throwable) {
                inspectOutOfMemoryError(throwable);
                if (!(throwable instanceof InterruptedException)) {
                    logger.warning("Exception occurred while sending events", throwable);
                }
            }
        }

        private void sendEvents() throws MalformedURLException {
            ArrayList<Event> eventList = new ArrayList<>();
            if (events.drainTo(eventList) == 0) {
                return;
            }

            URL url = new URL(cleanupUrl(managementCenterUrl) + "events.do");
            OutputStream outputStream = null;
            OutputStreamWriter writer = null;
            try {
                String clusterName = instance.getConfig().getClusterName();
                String address = instance.node.address.getHost() + ":" + instance.node.address.getPort();

                JsonObject batch = new EventBatch(clusterName, address, eventList).toJson();

                HttpURLConnection connection = openJsonConnection(url);
                outputStream = connection.getOutputStream();
                writer = new OutputStreamWriter(outputStream, UTF_8);

                batch.writeTo(writer);

                writer.flush();
                outputStream.flush();
                boolean success = post(connection);
                if (eventSendFailed && success) {
                    logger.info("Sent events to Management Center successfully.");
                    eventSendFailed = false;
                }
            } catch (Exception e) {
                if (!eventSendFailed) {
                    eventSendFailed = true;
                    log("Failed to send events to Management Center.", e);
                }
            } finally {
                closeResource(writer);
                closeResource(outputStream);
            }
        }
    }

    private final class PrepareStateThread extends Thread {

        private final long updateIntervalMs;

        private PrepareStateThread() {
            super(createThreadName(instance.getName(), "MC.State.Sender"));
            updateIntervalMs = calcUpdateInterval();
        }

        private long calcUpdateInterval() {
            long updateInterval = managementCenterConfig.getUpdateInterval();
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

    /**
     * Thread for sending cluster state to the Management Center.
     */
    private final class StateSendThread extends Thread {

        private final long updateIntervalMs;

        private String lastConfigETag;

        private StateSendThread() {
            super(createThreadName(instance.getName(), "MC.State.Sender"));
            updateIntervalMs = calcUpdateInterval();
        }

        private long calcUpdateInterval() {
            long updateInterval = managementCenterConfig.getUpdateInterval();
            return (updateInterval > 0) ? TimeUnit.SECONDS.toMillis(updateInterval) : DEFAULT_UPDATE_INTERVAL;
        }

        @Override
        public void run() {
            try {
                while (isRunning()) {
                    long startMs = Clock.currentTimeMillis();
                    sendStateAndReadConfig();
                    long endMs = Clock.currentTimeMillis();
                    sleepIfPossible(updateIntervalMs, endMs - startMs);
                }
            } catch (Throwable throwable) {
                inspectOutOfMemoryError(throwable);
                if (!(throwable instanceof InterruptedException)) {
                    logger.warning("Exception occurred while calculating stats", throwable);
                }
            }
        }

        private void sendStateAndReadConfig() throws MalformedURLException {
            URL url = newCollectorUrl();
            OutputStream outputStream = null;
            OutputStreamWriter writer = null;
            try {
                HttpURLConnection connection = openJsonConnection(url);
                if (lastConfigETag != null) {
                    connection.setRequestProperty("If-None-Match", lastConfigETag);
                }

                outputStream = connection.getOutputStream();
                writer = new OutputStreamWriter(outputStream, UTF_8);

                String memberStateJson = timedMemberStateJson.get();
                if (memberStateJson != null) {
                    writer.write(memberStateJson);
                    writer.flush();
                    outputStream.flush();

                    processResponse(connection);
                }
            } catch (Exception e) {
                if (!manCenterConnectionLost) {
                    manCenterConnectionLost = true;
                    log("Failed to connect to: " + url, e);
                    bwListConfigHandler.handleLostConnection();
                }
            } finally {
                closeResource(writer);
                closeResource(outputStream);
            }
        }

        private void processResponse(HttpURLConnection connection) throws Exception {
            int responseCode = connection.getResponseCode();
            boolean okResponse = responseCode == HTTP_SUCCESS || responseCode == HTTP_NOT_MODIFIED;
            if (!okResponse && !manCenterConnectionLost) {
                logger.warning("Failed to send response, responseCode:" + responseCode + " url:" + connection.getURL());
            }

            if (manCenterConnectionLost && okResponse) {
                logger.info("Connection to Management Center restored.");
                manCenterConnectionLost = false;
            } else if (!okResponse) {
                manCenterConnectionLost = true;
            }

            // process response only if config changed
            if (responseCode == HTTP_SUCCESS) {
                readAndApplyConfig(connection);
            }
        }

        private void readAndApplyConfig(HttpURLConnection connection) throws Exception {
            InputStream inputStream = null;
            InputStreamReader reader = null;
            try {
                inputStream = connection.getInputStream();
                reader = new InputStreamReader(inputStream, UTF_8);
                JsonObject response = Json.parse(reader).asObject();
                lastConfigETag = connection.getHeaderField("ETag");
                bwListConfigHandler.handleConfig(response);
            } finally {
                closeResource(reader);
                closeResource(inputStream);
            }
        }

        private URL newCollectorUrl() throws MalformedURLException {
            String url = cleanupUrl(managementCenterUrl) + "collector.do";
            return new URL(url);
        }
    }

    /**
     * Thread for polling tasks/requests from Management Center.
     */
    private final class TaskPollThread extends Thread {

        private final Map<Integer, Class<? extends ConsoleRequest>> consoleRequests = new HashMap<>();

        private final ExecutionService executionService = instance.node.getNodeEngine().getExecutionService();

        TaskPollThread() {
            super(createThreadName(instance.getName(), "MC.Task.Poller"));
            register(new ThreadDumpRequest());
            register(new ExecuteScriptRequest());
            register(new ConsoleCommandRequest());
            register(new RunGcRequest());
            register(new GetMapEntryRequest());
            if (JCacheDetector.isJCacheAvailable(instance.node.getNodeEngine().getConfigClassLoader(), logger)) {
                register(new GetCacheEntryRequest());
            } else {
                logger.finest("javax.cache api is not detected on classpath.Skip registering GetCacheEntryRequest...");
            }
            register(new TriggerPartialStartRequest());

            registerConfigRequests();
            registerClusterManagementRequests();
            registerWanRequests();
        }

        private void registerConfigRequests() {
            register(new GetMemberSystemPropertiesRequest());
            register(new MapConfigRequest());
            register(new MemberConfigRequest());
        }

        private void registerClusterManagementRequests() {
            register(new ClusterPropsRequest());
            register(new GetClusterStateRequest());
            register(new ChangeClusterStateRequest());
            register(new ShutdownClusterRequest());
            register(new PromoteMemberRequest());
            register(new ForceStartNodeRequest());
        }

        private void registerWanRequests() {
            register(new ChangeWanStateRequest());
            register(new ClearWanQueuesRequest());
            register(new WanCheckConsistencyRequest());
        }

        public void register(ConsoleRequest consoleRequest) {
            Class<? extends ConsoleRequest> reqClass = consoleRequests.put(consoleRequest.getType(), consoleRequest.getClass());
            if (reqClass != null) {
                throw new IllegalArgumentException("Request ID is already registered by " + reqClass);
            }
        }

        private HttpURLConnection openPostResponseConnection() throws IOException {
            URL url = newPostResponseUrl();
            if (logger.isFinestEnabled()) {
                logger.finest("Opening sendResponse connection:" + url);
            }
            HttpURLConnection connection = (HttpURLConnection) (connectionFactory != null
                    ? connectionFactory.openConnection(url)
                    : url.openConnection());
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(CONNECTION_TIMEOUT_MILLIS);
            connection.setReadTimeout(CONNECTION_TIMEOUT_MILLIS);
            return connection;
        }

        private URL newPostResponseUrl() throws MalformedURLException {
            return new URL(cleanupUrl(managementCenterUrl) + "putResponse.do");
        }

        @Override
        public void run() {
            try {
                while (isRunning()) {
                    processTask();
                    sleep();
                }
            } catch (Throwable throwable) {
                if (!(throwable instanceof InterruptedException)) {
                    inspectOutOfMemoryError(throwable);
                    logger.warning("Problem on Hazelcast Management Center Service while polling for a task.", throwable);
                }
            }
        }

        private void sleep() throws InterruptedException {
            Thread.sleep(SLEEP_BETWEEN_POLL_MILLIS);
        }

        private void processTask() {
            InputStream inputStream = null;
            InputStreamReader reader = null;
            try {
                inputStream = openTaskInputStream();
                reader = new InputStreamReader(inputStream, UTF_8);
                JsonObject request = Json.parse(reader).asObject();
                if (!request.isEmpty()) {
                    JsonObject innerRequest = getObject(request, "request");
                    final int type = getInt(innerRequest, "type");
                    final int taskId = getInt(request, "taskId");

                    Class<? extends ConsoleRequest> requestClass = consoleRequests.get(type);
                    if (requestClass == null) {
                        throw new RuntimeException("Failed to find a request for requestType: " + type);
                    }
                    ConsoleRequest task = requestClass.newInstance();
                    task.fromJson(getObject(innerRequest, "request"));
                    boolean success;
                    if (task instanceof AsyncConsoleRequest) {
                        executionService.execute(ASYNC_EXECUTOR, new AsyncConsoleRequestTask(taskId, task));
                        success = true;
                    } else {
                        success = processTaskAndSendResponse(taskId, task);
                    }
                    if (taskPollFailed && success) {
                        logger.info("Management Center task polling successful.");
                        taskPollFailed = false;
                    }
                }
            } catch (Exception e) {
                if (!taskPollFailed) {
                    taskPollFailed = true;
                    log("Failed to pull tasks from Management Center", e);
                }
            } finally {
                IOUtil.closeResource(reader);
                IOUtil.closeResource(inputStream);
            }
        }

        private boolean processTaskAndSendResponse(int taskId, ConsoleRequest task) throws Exception {
            HttpURLConnection connection = openPostResponseConnection();
            OutputStream outputStream = connection.getOutputStream();
            final OutputStreamWriter writer = new OutputStreamWriter(outputStream, UTF_8);
            try {
                JsonObject root = new JsonObject();
                root.add("taskId", taskId);
                root.add("type", task.getType());
                task.writeResponse(ManagementCenterService.this, root);
                root.writeTo(writer);
                writer.flush();
                outputStream.flush();
                return post(connection);
            } finally {
                closeResource(writer);
                closeResource(outputStream);
            }
        }

        private InputStream openTaskInputStream() throws IOException {
            URLConnection connection = openGetTaskConnection();
            return connection.getInputStream();
        }

        private URLConnection openGetTaskConnection() throws IOException {
            URL url = newGetTaskUrl();
            if (logger.isFinestEnabled()) {
                logger.finest("Opening getTask connection:" + url);
            }
            HttpURLConnection connection = (HttpURLConnection) (connectionFactory != null
                    ? connectionFactory.openConnection(url)
                    : url.openConnection());

            connection.setRequestProperty("Connection", "keep-alive");
            connection.setRequestMethod("POST");
            return connection;
        }

        private URL newGetTaskUrl() throws IOException {
            String clusterName = instance.getConfig().getClusterName();

            Address localAddress = instance.node.getClusterService().getLocalMember().getAddress();

            String urlString = cleanupUrl(managementCenterUrl) + "getTask.do?member=" + localAddress.getHost()
                    + ":" + localAddress.getPort() + "&cluster=" + encode(clusterName, "UTF-8");
            return new URL(urlString);
        }

        private class AsyncConsoleRequestTask implements Runnable {
            private final int taskId;
            private final ConsoleRequest task;

            AsyncConsoleRequestTask(int taskId, ConsoleRequest task) {
                this.taskId = taskId;
                this.task = task;
            }

            @Override
            public void run() {
                try {
                    processTaskAndSendResponse(taskId, task);
                } catch (Exception e) {
                    logger.warning("Problem while handling task: " + task, e);
                }
            }
        }
    }

    private void log(String msg, Throwable t) {
        if (logger.isFinestEnabled()) {
            logger.finest(msg, t);
        } else {
            logger.info(msg);
        }
    }

    /**
     * MembershipListener to send Management Center URL to the new members.
     */
    public class MemberListenerImpl implements MembershipListener {

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
            try {
                Member member = membershipEvent.getMember();
                if (member != null && instance.node.isMaster() && urlChanged) {
                    UpdateManagementCenterUrlOperation operation
                            = new UpdateManagementCenterUrlOperation(managementCenterUrl);
                    resolveFuture(callOnMember(member, operation));
                }
            } catch (Exception e) {
                logger.warning("Web server url cannot be send to the newly joined member", e);
            }
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        }
    }
}
