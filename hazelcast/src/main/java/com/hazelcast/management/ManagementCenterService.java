/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.ascii.rest.HttpCommand;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.management.operation.UpdateManagementCenterUrlOperation;
import com.hazelcast.management.request.ClusterPropsRequest;
import com.hazelcast.management.request.ConsoleCommandRequest;
import com.hazelcast.management.request.ConsoleRequest;
import com.hazelcast.management.request.EvictLocalMapRequest;
import com.hazelcast.management.request.ExecuteScriptRequest;
import com.hazelcast.management.request.GetLogsRequest;
import com.hazelcast.management.request.GetMapEntryRequest;
import com.hazelcast.management.request.GetMemberSystemPropertiesRequest;
import com.hazelcast.management.request.GetSystemWarningsRequest;
import com.hazelcast.management.request.MapConfigRequest;
import com.hazelcast.management.request.MemberConfigRequest;
import com.hazelcast.management.request.RunGcRequest;
import com.hazelcast.management.request.RuntimeStateRequest;
import com.hazelcast.management.request.ShutdownMemberRequest;
import com.hazelcast.management.request.ThreadDumpRequest;
import com.hazelcast.management.request.VersionMismatchLogRequest;
import com.hazelcast.map.MapService;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;
import static com.hazelcast.nio.IOUtil.closeResource;

public class ManagementCenterService {

    public static final int HTTP_SUCCESS = 200;

    private final HazelcastInstanceImpl instance;
    private final TaskPollThread taskPollThread;
    private final StateSendThread stateSendThread;
    private final ILogger logger;

    private final ConsoleCommandHandler commandHandler;
    private final ManagementCenterConfig managementCenterConfig;
    private final SerializationService serializationService;
    private final ManagementCenterIdentifier identifier;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private volatile String managementCenterUrl;
    private volatile boolean urlChanged = false;
    private volatile boolean versionMismatch = false;

    public ManagementCenterService(HazelcastInstanceImpl instance) {
        this.instance = instance;
        logger = instance.node.getLogger(ManagementCenterService.class);
        managementCenterConfig = getManagementCenterConfig();
        managementCenterUrl = getManagementCenterUrl();
        commandHandler = new ConsoleCommandHandler(instance);
        taskPollThread = new TaskPollThread();
        stateSendThread = new StateSendThread();
        serializationService = instance.node.getSerializationService();
        identifier = newManagementCenterIdentifier();
        registerListeners();
    }


    private String getManagementCenterUrl() {
        return managementCenterConfig.getUrl();
    }


    private GroupProperties getGroupProperties() {
        return instance.node.getGroupProperties();
    }


    private void registerListeners() {
        if (!managementCenterConfig.isEnabled()) {
            return;
        }
        instance.getLifecycleService().addLifecycleListener(new LifecycleListenerImpl());
        instance.getCluster().addMembershipListener(new MemberListenerImpl());
    }


    private ManagementCenterConfig getManagementCenterConfig() {
        ManagementCenterConfig config = instance.node.config.getManagementCenterConfig();
        if (config == null) {
            throw new IllegalStateException("ManagementCenterConfig can't be null!");
        }
        return config;
    }

    private ManagementCenterIdentifier newManagementCenterIdentifier() {
        Address address = instance.node.address;
        String groupName = instance.getConfig().getGroupConfig().getName();
        String version = instance.node.getBuildInfo().getVersion();
        return new ManagementCenterIdentifier(version, groupName, address.getHost() + ":" + address.getPort());
    }

    private static String cleanupUrl(String url) {
        if (url == null) {
            return null;
        }
        return url.endsWith("/") ? url : url + '/';
    }

    public void start() {
        if (managementCenterUrl == null) {
            logger.warning("Can't start Hazelcast Management Center Service: web-server URL is null!");
            return;
        }

        if (!isRunning.compareAndSet(false, true)) {
            //it is already started
            return;
        }

        taskPollThread.start();
        stateSendThread.start();
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
        } catch (Throwable ignored) {
        }
    }

    public byte[] clusterWideUpdateManagementCenterUrl(String groupName, String groupPass, String newUrl) {
        try {
            GroupConfig groupConfig = instance.getConfig().getGroupConfig();
            if (!(groupConfig.getName().equals(groupName) && groupConfig.getPassword().equals(groupPass))) {
                return HttpCommand.RES_403;
            }

            final Collection<MemberImpl> memberList = instance.node.clusterService.getMemberList();
            for (MemberImpl member : memberList) {
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
        logger.info("Management Center URL has changed. " +
                "Hazelcast will connect to Management Center on address: \n" + managementCenterUrl);
    }

    private void interruptThread(Thread t) {
        if (t != null) {
            t.interrupt();
        }
    }

    public void signalVersionMismatch() {
        versionMismatch = true;
    }

    public Object callOnAddress(Address address, Operation operation) {
        //todo: why are we always executing on the mapservice??
        OperationService operationService = instance.node.nodeEngine.getOperationService();
        Future future = operationService.invokeOnTarget(MapService.SERVICE_NAME, operation, address);
        try {
            return future.get();
        } catch (Throwable t) {
            StringWriter s = new StringWriter();
            t.printStackTrace(new PrintWriter(s));
            return s.toString();
        }
    }
    public Object callOnThis(Operation operation) {
        return callOnAddress(instance.node.getThisAddress(), operation);
    }

    public Object callOnMember(Member member, Operation operation) {
        Address address = ((MemberImpl) member).getAddress();
        return callOnAddress(address, operation);
    }

    public void send(Address address, Operation operation) {
        //todo: clean up needed.
        OperationService operationService = instance.node.nodeEngine.getOperationService();

        operationService.createInvocationBuilder(MapService.SERVICE_NAME, operation, address).invoke();
    }

    public HazelcastInstanceImpl getHazelcastInstance() {
        return instance;
    }

    public ConsoleCommandHandler getCommandHandler() {
        return commandHandler;
    }

    private boolean isRunning() {
        return isRunning.get();
    }

    private void post(HttpURLConnection connection) throws IOException {
        //we need to call 'getResponseCode'. If we don't the data placed in the outputstream, will not be send to the
        //managementcenter. For more information see:
        //http://stackoverflow.com/questions/4844535/why-do-you-have-to-call-urlconnectiongetinputstream-to-be-able-to-write-out-to
        int responseCode = connection.getResponseCode();

        if (responseCode != HTTP_SUCCESS) {
            logger.warning("Failed to send response, responseCode:" + responseCode + " url:" + connection.getURL());
        }
    }

    private void sleepOnVersionMismatch() throws InterruptedException {
        if (versionMismatch) {
            Thread.sleep(1000 * 60);
            versionMismatch = false;
        }
    }

    private class StateSendThread extends Thread {
        private final TimedMemberStateFactory timedMemberStateFactory;
        private final int updateIntervalMs;

        private StateSendThread() {
            super(instance.getThreadGroup(), instance.node.getThreadNamePrefix("MC.State.Sender"));
            timedMemberStateFactory = new TimedMemberStateFactory(instance);
            updateIntervalMs = calcUpdateInterval();
        }

        private int calcUpdateInterval() {
            int updateInterval = managementCenterConfig.getUpdateInterval();
            return updateInterval > 0 ? updateInterval * 1000 : 5000;
        }

        @Override
        public void run() {
            try {
                while (isRunning()) {
                    sleepOnVersionMismatch();
                    sendState();
                    sleep();
                }
            } catch (Throwable throwable) {
                inspectOutputMemoryError(throwable);
                logger.warning("Hazelcast Management Center Service will be shutdown due to exception.", throwable);
                shutdown();
            }
        }

        private void sleep() throws InterruptedException {
            Thread.sleep(updateIntervalMs);
        }

        private void sendState() throws InterruptedException, MalformedURLException {
            URL url = newCollectorUrl();
            try {
                //todo: does the connection not need to be closed?
                HttpURLConnection connection = openConnection(url);
                OutputStream outputStream = connection.getOutputStream();
                try {
                    JsonObject root = new JsonObject();
                    root.add("identifier", identifier.toJson());
                    TimedMemberState timedMemberState = timedMemberStateFactory.createTimedMemberState();
                    root.add("timedMemberState", timedMemberState.toJson());
                    outputStream.write(root.toString().getBytes());
                    outputStream.flush();
                    post(connection);
                } finally {
                    closeResource(outputStream);
                }
            } catch (ConnectException e) {
                if (logger.isFinestEnabled()) {
                    logger.finest(e);
                } else {
                    logger.info("Failed to connect to:" + url);
                }
            } catch (Exception e) {
                logger.warning(e);
            }
        }

        private HttpURLConnection openConnection(URL url) throws IOException {
            if (logger.isFinestEnabled()) {
                logger.finest("Opening collector connection:" + url);
            }

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestMethod("POST");
            return connection;
        }

        private URL newCollectorUrl() throws MalformedURLException {
            String url = cleanupUrl(managementCenterUrl) + "collector.do";
            return new URL(url);
        }
    }

    private class TaskPollThread extends Thread {
        private final Map<Integer, Class<? extends ConsoleRequest>> consoleRequests =
                new HashMap<Integer, Class<? extends ConsoleRequest>>();
        private final Random rand = new Random();

        TaskPollThread() {
            super(instance.node.threadGroup, instance.node.getThreadNamePrefix("MC.Task.Poller"));
            register(new RuntimeStateRequest());
            register(new ThreadDumpRequest());
            register(new ExecuteScriptRequest());
            register(new EvictLocalMapRequest());
            register(new ConsoleCommandRequest());
            register(new MapConfigRequest());
            register(new MemberConfigRequest());
            register(new ClusterPropsRequest());
            register(new GetLogsRequest());
            register(new RunGcRequest());
            register(new GetMemberSystemPropertiesRequest());
            register(new GetMapEntryRequest());
            register(new VersionMismatchLogRequest());
            register(new ShutdownMemberRequest());
            register(new GetSystemWarningsRequest());
        }

        public void register(ConsoleRequest consoleRequest) {
            consoleRequests.put(consoleRequest.getType(), consoleRequest.getClass());
        }


        private HttpURLConnection openPostResponseConnection() throws IOException {
            URL url = newPostResponseUrl();
            if (logger.isFinestEnabled()) {
                logger.finest("Opening sendResponse connection:" + url);
            }
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(2000);
            connection.setReadTimeout(2000);
            return connection;
        }

        private URL newPostResponseUrl() throws MalformedURLException {
            return new URL(cleanupUrl(managementCenterUrl) + "putResponse.do");
        }

        @Override
        public void run() {
            try {
                while (isRunning()) {
                    sleepOnVersionMismatch();
                    processTask();
                    sleep();
                }
            } catch (Throwable throwable) {
                inspectOutputMemoryError(throwable);
                logger.warning("Problem on Hazelcast Management Center Service while polling for a task.", throwable);
            }
        }

        private void sleep() throws InterruptedException {
            //todo: magic numbers are no good.
            //todo: why the random part
            //todo: we want configurable frequency for task polling
            Thread.sleep(700 + rand.nextInt(300));
        }

        private void processTask() {
            InputStream inputStream = null;
            try {
                //todo: don't we need to close the connection?
                inputStream = openTaskInputStream();
                BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));

                StringBuilder sb = new StringBuilder();
                String input;
                while ((input = in.readLine()) != null) {
                    sb.append(input);
                }

                JsonObject request = JsonObject.readFrom(sb.toString());
                System.out.println("request = " + request);
                if (!request.isEmpty()){
                    JsonObject innerRequest = request.get("request").asObject();
                    final int type = innerRequest.get("type").asInt();
                    final int taskId = request.get("taskId").asInt();

                    Class<? extends ConsoleRequest> requestClass = consoleRequests.get(type);
                    if (requestClass == null) {
                        throw new RuntimeException("Failed to find a request for requestType:" + type);
                    }
                    ConsoleRequest task = requestClass.newInstance();
                    task.fromJson(innerRequest.get("request").asObject());
                    processTaskAndSendResponse(taskId, task);
                }

            } catch (Exception e) {
                //todo: even if there is an internal error with the task, we don't see it. That is kinda shitty
                logger.warning(e);
            } finally {
                IOUtil.closeResource(inputStream);
            }
        }

        public void processTaskAndSendResponse(int taskId, ConsoleRequest task) {
            try {
                //todo: don't we need to close this connection?
                HttpURLConnection connection = openPostResponseConnection();
                OutputStream outputStream = connection.getOutputStream();
                try {
                    JsonObject root = new JsonObject();
                    root.add("identifier", identifier.toJson());
                    root.add("taskId", taskId);
                    root.add("type", task.getType());
                    task.writeResponse(ManagementCenterService.this, root);
                    outputStream.write(root.toString().getBytes());
                    outputStream.flush();
                    post(connection);
                } finally {
                    closeResource(outputStream);
                }
            } catch (Exception e) {
                logger.warning("Failed process task:" + task, e);
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

            URLConnection connection = url.openConnection();
            //todo: why do we set this property if the connection is not going to be re-used?
            connection.setRequestProperty("Connection", "keep-alive");
            return connection;
        }

        private URL newGetTaskUrl() throws MalformedURLException {
            GroupConfig groupConfig = instance.getConfig().getGroupConfig();

            Address localAddress = ((MemberImpl) instance.node.getClusterService().getLocalMember()).getAddress();

            String urlString = cleanupUrl(managementCenterUrl) + "getTask.do?member=" + localAddress.getHost()
                    + ":" + localAddress.getPort() + "&cluster=" + groupConfig.getName();

            return new URL(urlString);
        }
    }


    private class LifecycleListenerImpl implements LifecycleListener {

        @Override
        public void stateChanged(final LifecycleEvent event) {
            if (event.getState() == LifecycleState.STARTED) {
                try {
                    start();
                } catch (Exception e) {
                    logger.severe("ManagementCenterService could not be started!", e);
                }
            }
        }
    }

    public class MemberListenerImpl implements MembershipListener {

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
            try {
                Member member = membershipEvent.getMember();
                if (member != null && instance.node.isMaster() && urlChanged) {
                    Operation operation = new UpdateManagementCenterUrlOperation(managementCenterUrl);
                    callOnMember(member, operation);
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
