/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.ascii.rest;

import com.eclipsesource.json.Json;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.Member;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.dto.WanReplicationConfigDTO;
import com.hazelcast.internal.management.operation.AddWanConfigOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanReplicationService;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.StringUtil.stringToBytes;

public class HttpPostCommandProcessor extends HttpCommandProcessor<HttpPostCommand> {
    private static final byte[] QUEUE_SIMPLE_VALUE_CONTENT_TYPE = stringToBytes("text/plain");
    private final ILogger logger;


    public HttpPostCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
        this.logger = textCommandService.getNode().getLogger(HttpPostCommandProcessor.class);
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity"})
    public void handle(HttpPostCommand command) {
        try {
            String uri = command.getURI();
            if (uri.startsWith(URI_MAPS)) {
                handleMap(command, uri);
            } else if (uri.startsWith(URI_MANCENTER_CHANGE_URL)) {
                handleManagementCenterUrlChange(command);
            } else if (uri.startsWith(URI_QUEUES)) {
                handleQueue(command, uri);
            } else if (uri.startsWith(URI_CLUSTER_STATE_URL)) {
                handleGetClusterState(command);
            } else if (uri.startsWith(URI_CHANGE_CLUSTER_STATE_URL)) {
                handleChangeClusterState(command);
            } else if (uri.startsWith(URI_CLUSTER_VERSION_URL)) {
                handleChangeClusterVersion(command);
            } else if (uri.startsWith(URI_SHUTDOWN_CLUSTER_URL)) {
                handleClusterShutdown(command);
                return;
            } else if (uri.startsWith(URI_FORCESTART_CLUSTER_URL)) {
                handleForceStart(command);
            } else if (uri.startsWith(URI_HOT_RESTART_BACKUP_INTERRUPT_CLUSTER_URL)) {
                handleHotRestartBackupInterrupt(command);
            } else if (uri.startsWith(URI_HOT_RESTART_BACKUP_CLUSTER_URL)) {
                handleHotRestartBackup(command);
            } else if (uri.startsWith(URI_PARTIALSTART_CLUSTER_URL)) {
                handlePartialStart(command);
            } else if (uri.startsWith(URI_CLUSTER_NODES_URL)) {
                handleListNodes(command);
            } else if (uri.startsWith(URI_SHUTDOWN_NODE_CLUSTER_URL)) {
                handleShutdownNode(command);
            } else if (uri.startsWith(URI_WAN_SYNC_MAP)) {
                handleWanSyncMap(command);
            } else if (uri.startsWith(URI_WAN_SYNC_ALL_MAPS)) {
                handleWanSyncAllMaps(command);
            } else if (uri.startsWith(URI_MANCENTER_WAN_CLEAR_QUEUES)) {
                handleWanClearQueues(command);
            } else if (uri.startsWith(URI_ADD_WAN_CONFIG)) {
                handleAddWanConfig(command);
            } else {
                command.setResponse(HttpCommand.RES_400);
            }
        } catch (Exception e) {
            command.setResponse(HttpCommand.RES_500);
        }
        textCommandService.sendResponse(command);
    }

    private void handleChangeClusterState(HttpPostCommand command) throws UnsupportedEncodingException {
        byte[] data = command.getData();
        String[] strList = bytesToString(data).split("&");
        String groupName = URLDecoder.decode(strList[0], "UTF-8");
        String groupPass = URLDecoder.decode(strList[1], "UTF-8");
        String stateParam = URLDecoder.decode(strList[2], "UTF-8");
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            GroupConfig groupConfig = node.getConfig().getGroupConfig();
            if (!(groupConfig.getName().equals(groupName) && groupConfig.getPassword().equals(groupPass))) {
                res = response(ResponseType.FORBIDDEN);
            } else {
                ClusterState state = clusterService.getClusterState();
                if (stateParam.equals("frozen")) {
                    state = ClusterState.FROZEN;
                }
                if (stateParam.equals("active")) {
                    state = ClusterState.ACTIVE;
                }
                if (stateParam.equals("passive")) {
                    state = ClusterState.PASSIVE;
                }
                if (!state.equals(clusterService.getClusterState())) {
                    clusterService.changeClusterState(state);
                    res = response(ResponseType.SUCCESS, "state", state.toString().toLowerCase());
                } else {
                    res = response(ResponseType.FAIL, "state", state.toString().toLowerCase());
                }
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while changing cluster state", throwable);
            res = exceptionResponse(throwable);
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    private void handleGetClusterState(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            if (!checkCredentials(command)) {
                res = response(ResponseType.FORBIDDEN);
            } else {
                res = response(ResponseType.SUCCESS, "state", clusterService.getClusterState().toString().toLowerCase());
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while getting cluster state", throwable);
            res = exceptionResponse(throwable);
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    private void handleChangeClusterVersion(HttpPostCommand command) throws UnsupportedEncodingException {
        byte[] data = command.getData();
        String[] strList = bytesToString(data).split("&");
        String groupName = URLDecoder.decode(strList[0], "UTF-8");
        String groupPass = URLDecoder.decode(strList[1], "UTF-8");
        String versionParam = URLDecoder.decode(strList[2], "UTF-8");
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            GroupConfig groupConfig = node.getConfig().getGroupConfig();
            if (!(groupConfig.getName().equals(groupName) && groupConfig.getPassword().equals(groupPass))) {
                res = response(ResponseType.FORBIDDEN);
            } else {
                Version version;
                try {
                    version = Version.of(versionParam);
                    clusterService.changeClusterVersion(version);
                    res = response(ResponseType.SUCCESS, "version", clusterService.getClusterVersion().toString());
                } catch (Exception ex) {
                    res = response(ResponseType.FAIL, "version", clusterService.getClusterVersion().toString());
                }
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while changing cluster version", throwable);
            res = exceptionResponse(throwable);
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    private void handleForceStart(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        try {
            Node node = textCommandService.getNode();
            if (!checkCredentials(command)) {
                res = response(ResponseType.FORBIDDEN);
            } else {
                boolean success = node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
                res = response(success ? ResponseType.SUCCESS : ResponseType.FAIL);
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while handling force start", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(command, res);
    }

    private void handlePartialStart(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        try {
            Node node = textCommandService.getNode();
            if (!checkCredentials(command)) {
                res = response(ResponseType.FORBIDDEN);
            } else {
                boolean success = node.getNodeExtension().getInternalHotRestartService().triggerPartialStart();
                res = response(success ? ResponseType.SUCCESS : ResponseType.FAIL);
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while handling partial start", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(command, res);
    }

    private void handleHotRestartBackup(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        try {
            if (checkCredentials(command)) {
                textCommandService.getNode().getNodeExtension().getHotRestartService().backup();
                res = response(ResponseType.SUCCESS);
            } else {
                res = response(ResponseType.FORBIDDEN);
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while invoking hot backup", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(command, res);
    }

    private void handleHotRestartBackupInterrupt(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        try {
            if (checkCredentials(command)) {
                textCommandService.getNode().getNodeExtension().getHotRestartService().interruptBackupTask();
                res = response(ResponseType.SUCCESS);
            } else {
                res = response(ResponseType.FORBIDDEN);
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while interrupting hot backup", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(command, res);
    }

    private void handleClusterShutdown(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            if (!checkCredentials(command)) {
                res = response(ResponseType.FORBIDDEN);
            } else {
                res = response(ResponseType.SUCCESS);
                sendResponse(command, res);
                clusterService.shutdown();
                return;
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while shutting down cluster", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(command, res);
    }

    private void handleListNodes(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            if (!checkCredentials(command)) {
                res = response(ResponseType.FORBIDDEN);
            } else {
                final String responseTxt = clusterService.getMembers().toString() + "\n"
                        + BuildInfoProvider.getBuildInfo().getVersion() + "\n"
                        + System.getProperty("java.version");
                res = response(ResponseType.SUCCESS, "response", responseTxt);
                sendResponse(command, res);
                return;
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while listing nodes", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(command, res);
    }

    private void handleShutdownNode(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        try {
            Node node = textCommandService.getNode();
            if (!checkCredentials(command)) {
                res = response(ResponseType.FORBIDDEN);
            } else {
                res = response(ResponseType.SUCCESS);
                sendResponse(command, res);
                node.hazelcastInstance.shutdown();
                return;
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while shutting down", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(command, res);
    }

    private void handleQueue(HttpPostCommand command, String uri) {
        String simpleValue = null;
        String suffix;
        if (uri.endsWith("/")) {
            suffix = uri.substring(URI_QUEUES.length(), uri.length() - 1);
        } else {
            suffix = uri.substring(URI_QUEUES.length(), uri.length());
        }
        int indexSlash = suffix.lastIndexOf('/');

        String queueName;
        if (indexSlash == -1) {
            queueName = suffix;
        } else {
            queueName = suffix.substring(0, indexSlash);
            simpleValue = suffix.substring(indexSlash + 1, suffix.length());
        }
        byte[] data;
        byte[] contentType;
        if (simpleValue == null) {
            data = command.getData();
            contentType = command.getContentType();
        } else {
            data = stringToBytes(simpleValue);
            contentType = QUEUE_SIMPLE_VALUE_CONTENT_TYPE;
        }
        boolean offerResult = textCommandService.offer(queueName, new RestValue(data, contentType));
        if (offerResult) {
            command.send200();
        } else {
            command.setResponse(HttpCommand.RES_503);
        }
    }

    private void handleManagementCenterUrlChange(HttpPostCommand command) throws UnsupportedEncodingException {
        if (textCommandService.getNode().getProperties().getBoolean(GroupProperty.MC_URL_CHANGE_ENABLED)) {
            byte[] res = HttpCommand.RES_204;
            byte[] data = command.getData();
            String[] strList = bytesToString(data).split("&");
            String cluster = URLDecoder.decode(strList[0], "UTF-8");
            String pass = URLDecoder.decode(strList[1], "UTF-8");
            String url = URLDecoder.decode(strList[2], "UTF-8");

            ManagementCenterService managementCenterService = textCommandService.getNode().getManagementCenterService();
            if (managementCenterService != null) {
                res = managementCenterService.clusterWideUpdateManagementCenterUrl(cluster, pass, url);
            }
            command.setResponse(res);
        } else {
            command.setResponse(HttpCommand.RES_503);
        }
    }

    private void handleMap(HttpPostCommand command, String uri) {
        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        String mapName = uri.substring(URI_MAPS.length(), indexEnd);
        String key = uri.substring(indexEnd + 1);
        byte[] data = command.getData();
        textCommandService.put(mapName, key, new RestValue(data, command.getContentType()), -1);
        command.send200();
    }

    private void handleWanSyncMap(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        byte[] data = command.getData();
        String[] strList = bytesToString(data).split("&");
        String wanRepName = URLDecoder.decode(strList[0], "UTF-8");
        String targetGroup = URLDecoder.decode(strList[1], "UTF-8");
        String mapName = URLDecoder.decode(strList[2], "UTF-8");
        try {
            textCommandService.getNode().getNodeEngine().getWanReplicationService().syncMap(wanRepName, targetGroup, mapName);
            res = response(ResponseType.SUCCESS, "message", "Sync initiated");
        } catch (Exception ex) {
            logger.warning("Error occurred while syncing map", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(command, res);
    }

    private void handleWanSyncAllMaps(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        byte[] data = command.getData();
        String[] strList = bytesToString(data).split("&");
        String wanRepName = URLDecoder.decode(strList[0], "UTF-8");
        String targetGroup = URLDecoder.decode(strList[1], "UTF-8");
        try {
            textCommandService.getNode().getNodeEngine().getWanReplicationService().syncAllMaps(wanRepName, targetGroup);
            res = response(ResponseType.SUCCESS, "message", "Sync initiated");
        } catch (Exception ex) {
            logger.warning("Error occurred while syncing maps", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(command, res);
    }

    private void handleWanClearQueues(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        byte[] data = command.getData();
        String[] strList = bytesToString(data).split("&");
        String wanRepName = URLDecoder.decode(strList[0], "UTF-8");
        String targetGroup = URLDecoder.decode(strList[1], "UTF-8");
        try {
            textCommandService.getNode().getNodeEngine().getWanReplicationService().clearQueues(wanRepName, targetGroup);
            res = response(ResponseType.SUCCESS, "message", "WAN replication queues are cleared.");
        } catch (Exception ex) {
            logger.warning("Error occurred while clearing queues", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(command, res);
    }

    private void handleAddWanConfig(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        byte[] data = command.getData();
        String[] strList = bytesToString(data).split("&");
        String wanConfigJson = URLDecoder.decode(strList[0], "UTF-8");
        try {
            OperationService opService = textCommandService.getNode().getNodeEngine().getOperationService();
            final Set<Member> members = textCommandService.getNode().getClusterService().getMembers();
            WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
            WanReplicationConfigDTO dto = new WanReplicationConfigDTO(wanReplicationConfig);
            dto.fromJson(Json.parse(wanConfigJson).asObject());
            List<InternalCompletableFuture> futureList = new ArrayList<InternalCompletableFuture>(members.size());
            for (Member member : members) {
                InternalCompletableFuture<Object> future = opService.invokeOnTarget(WanReplicationService.SERVICE_NAME,
                        new AddWanConfigOperation(dto.getConfig()), member.getAddress());
                futureList.add(future);
            }
            for (InternalCompletableFuture future : futureList) {
                future.get();
            }
            res = response(ResponseType.SUCCESS, "message", "WAN configuration added.");
        } catch (Exception ex) {
            logger.warning("Error occurred while adding WAN config", ex);
            res = exceptionResponse(ex);
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    private static String exceptionResponse(Throwable throwable) {
        return response(ResponseType.FAIL, "message", throwable.getMessage());
    }

    private static String response(ResponseType type, String... attributes) {
        final StringBuilder builder = new StringBuilder("{");
        builder.append("\"status\":\"").append(type).append("\"");
        if (attributes.length > 0) {
            for (int i = 0; i < attributes.length; ) {
                final String key = attributes[i++];
                final String value = attributes[i++];
                if (value != null) {
                    builder.append(String.format(",\"%s\":\"%s\"", key, value));
                }
            }
        }
        return builder.append("}").toString();
    }

    private enum ResponseType {
        SUCCESS, FAIL, FORBIDDEN;

        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }
    }

    private boolean checkCredentials(HttpPostCommand command) throws UnsupportedEncodingException {
        byte[] data = command.getData();
        final String[] strList = bytesToString(data).split("&");
        final String groupName = URLDecoder.decode(strList[0], "UTF-8");
        final String groupPass = URLDecoder.decode(strList[1], "UTF-8");
        final GroupConfig groupConfig = textCommandService.getNode().getConfig().getGroupConfig();
        return groupConfig.getName().equals(groupName) && groupConfig.getPassword().equals(groupPass);
    }

    private void sendResponse(HttpPostCommand command, String value) {
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(value));
        textCommandService.sendResponse(command);
    }

    @Override
    public void handleRejection(HttpPostCommand command) {
        handle(command);
    }
}
