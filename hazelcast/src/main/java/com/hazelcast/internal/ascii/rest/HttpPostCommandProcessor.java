/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.dto.WanReplicationConfigDTO;
import com.hazelcast.internal.management.operation.AddWanConfigOperation;
import com.hazelcast.internal.management.request.UpdatePermissionConfigRequest;
import com.hazelcast.internal.util.rest.RestUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.SecurityService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.StringUtil;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanReplicationService;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.util.rest.RestUtil.decodeParams;
import static com.hazelcast.internal.util.rest.RestUtil.exceptionResponse;
import static com.hazelcast.internal.util.rest.RestUtil.response;
import static com.hazelcast.internal.util.rest.RestUtil.sendResponse;
import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.StringUtil.stringToBytes;
import static com.hazelcast.util.StringUtil.upperCaseInternal;

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
            } else if (uri.startsWith(URI_CHANGE_CLUSTER_STATE_URL)) {
                handleChangeClusterState(command);
            } else if (uri.startsWith(URI_CLUSTER_VERSION_URL)) {
                handleChangeClusterVersion(command);
            } else if (uri.startsWith(URI_WAN_SYNC_MAP) || uri.startsWith(LEGACY_URI_WAN_SYNC_MAP)) {
                handleWanSyncMap(command);
            } else if (uri.startsWith(URI_WAN_SYNC_ALL_MAPS) || uri.startsWith(LEGACY_URI_WAN_SYNC_ALL_MAPS)) {
                handleWanSyncAllMaps(command);
            } else if (uri.startsWith(URI_MANCENTER_WAN_CLEAR_QUEUES) || uri.startsWith(LEGACY_URI_MANCENTER_WAN_CLEAR_QUEUES)) {
                handleWanClearQueues(command);
            } else if (uri.startsWith(URI_ADD_WAN_CONFIG) || uri.startsWith(LEGACY_URI_ADD_WAN_CONFIG)) {
                handleAddWanConfig(command);
            } else if (uri.startsWith(URI_WAN_PAUSE_PUBLISHER)) {
                handleWanPausePublisher(command);
            } else if (uri.startsWith(URI_WAN_STOP_PUBLISHER)) {
                handleWanStopPublisher(command);
            } else if (uri.startsWith(URI_WAN_RESUME_PUBLISHER)) {
                handleWanResumePublisher(command);
            } else if (uri.startsWith(URI_WAN_CONSISTENCY_CHECK_MAP)) {
                handleWanConsistencyCheck(command);
            } else if (uri.startsWith(URI_UPDATE_PERMISSIONS)) {
                handleUpdatePermissions(command);
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
            } else if (uri.startsWith(URI_SHUTDOWN_NODE_CLUSTER_URL)) {
                handleShutdownNode(command);
                /**
                 * The below handle methods are moved to HttpCommandProcessor to use them at {@link #HttpGetCommandProcessor}
                 * so these else-if blocks are @Deprecated.
                 * We are keeping them for backward-compatible by linking functionality(allowing both GET & POST)
                 * These blocks will be removed with future releases.
                 */
            } else if (uri.startsWith(URI_CLUSTER_STATE_URL)) {
                handleGetClusterState(command);
            } else if (uri.startsWith(URI_CLUSTER_NODES_URL)) {
                handleListNodes(command);
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
        String stateParam = URLDecoder.decode(strList[0], "UTF-8");
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            ClusterState state = ClusterState.valueOf(upperCaseInternal(stateParam));
            if (!state.equals(clusterService.getClusterState())) {
                clusterService.changeClusterState(state);
                res = response(RestUtil.ResponseType.SUCCESS, "state", state.toString().toLowerCase(StringUtil.LOCALE_INTERNAL));
            } else {
                res = response(RestUtil.ResponseType.FAIL, "state", state.toString().toLowerCase(StringUtil.LOCALE_INTERNAL));
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while changing cluster state", throwable);
            res = exceptionResponse(throwable);
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    private void handleChangeClusterVersion(HttpPostCommand command) throws UnsupportedEncodingException {
        byte[] data = command.getData();
        String[] strList = bytesToString(data).split("&");
        String versionParam = URLDecoder.decode(strList[0], "UTF-8");
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            Version version = Version.of(versionParam);
            clusterService.changeClusterVersion(version);
            res = response(RestUtil.ResponseType.SUCCESS, "version", clusterService.getClusterVersion().toString());
        } catch (Throwable throwable) {
            logger.warning("Error occurred while changing cluster version", throwable);
            res = exceptionResponse(throwable);
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    public void handleClusterShutdown(HttpPostCommand command) {
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            res = response(RestUtil.ResponseType.SUCCESS);
            sendResponse(textCommandService, command, res);
            clusterService.shutdown();
            return;
        } catch (Throwable throwable) {
            logger.warning("Error occurred while shutting down cluster", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(textCommandService, command, res);
    }

    public void handleShutdownNode(HttpPostCommand command) {
        String res;
        try {
            Node node = textCommandService.getNode();
            res = response(RestUtil.ResponseType.SUCCESS);
            sendResponse(textCommandService, command, res);
            node.hazelcastInstance.shutdown();
            return;
        } catch (Throwable throwable) {
            logger.warning("Error occurred while shutting down", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(textCommandService, command, res);
    }

    public void handleForceStart(HttpPostCommand command) {
        String res;
        try {
            Node node = textCommandService.getNode();
            boolean success = node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
            res = response(success ? RestUtil.ResponseType.SUCCESS : RestUtil.ResponseType.FAIL);
        } catch (Throwable throwable) {
            logger.warning("Error occurred while handling force start", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(textCommandService, command, res);
    }

    public void handlePartialStart(HttpPostCommand command) {
        String res;
        try {
            Node node = textCommandService.getNode();
            boolean success = node.getNodeExtension().getInternalHotRestartService().triggerPartialStart();
            res = response(success ? RestUtil.ResponseType.SUCCESS : RestUtil.ResponseType.FAIL);
        } catch (Throwable throwable) {
            logger.warning("Error occurred while handling partial start", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(textCommandService, command, res);
    }

    public void handleHotRestartBackup(HttpPostCommand command) {
        String res;
        try {
            textCommandService.getNode().getNodeExtension().getHotRestartService().backup();
            res = response(RestUtil.ResponseType.SUCCESS);
        } catch (Throwable throwable) {
            logger.warning("Error occurred while invoking hot backup", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(textCommandService, command, res);
    }

    public void handleHotRestartBackupInterrupt(HttpPostCommand command) {
        String res;
        try {
            textCommandService.getNode().getNodeExtension().getHotRestartService().interruptBackupTask();
            res = response(RestUtil.ResponseType.SUCCESS);
        } catch (Throwable throwable) {
            logger.warning("Error occurred while interrupting hot backup", throwable);
            res = exceptionResponse(throwable);
        }
        sendResponse(textCommandService, command, res);
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
            String url = URLDecoder.decode(strList[0], "UTF-8");

            ManagementCenterService managementCenterService = textCommandService.getNode().getManagementCenterService();
            if (managementCenterService != null) {
                res = managementCenterService.clusterWideUpdateManagementCenterUrl(url);
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

    /**
     * Initiates a WAN sync for a single map and the wan replication name and publisher ID defined
     * by the command parameters.
     *
     * @param command the HTTP command
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but
     *                                      named character encoding is not supported
     */
    private void handleWanSyncMap(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        final String[] params = decodeParams(command, 3);
        final String wanRepName = params[0];
        final String publisherId = params[1];
        final String mapName = params[2];
        try {
            textCommandService.getNode().getNodeEngine().getWanReplicationService().syncMap(wanRepName, publisherId, mapName);
            res = response(RestUtil.ResponseType.SUCCESS, "message", "Sync initiated");
        } catch (Exception ex) {
            logger.warning("Error occurred while syncing map", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(textCommandService, command, res);
    }

    /**
     * Initiates WAN sync for all maps and the wan replication name and publisher ID
     * defined
     * by the command parameters.
     *
     * @param command the HTTP command
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but
     *                                      named character encoding is not supported
     */
    private void handleWanSyncAllMaps(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        final String[] params = decodeParams(command, 2);
        final String wanRepName = params[0];
        final String publisherId = params[1];
        try {
            textCommandService.getNode().getNodeEngine().getWanReplicationService().syncAllMaps(wanRepName, publisherId);
            res = response(RestUtil.ResponseType.SUCCESS, "message", "Sync initiated");
        } catch (Exception ex) {
            logger.warning("Error occurred while syncing maps", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(textCommandService, command, res);
    }

    /**
     * Initiates a WAN consistency check for a single map and the WAN replication
     * name and publisher ID defined by the command parameters.
     *
     * @param command the HTTP command
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but
     *                                      named character encoding is not supported
     */
    private void handleWanConsistencyCheck(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        String[] params = decodeParams(command, 3);
        String wanReplicationName = params[0];
        String publisherId = params[1];
        String mapName = params[2];
        WanReplicationService service = textCommandService.getNode().getNodeEngine().getWanReplicationService();

        try {
            service.consistencyCheck(wanReplicationName, publisherId, mapName);
            res = response(RestUtil.ResponseType.SUCCESS, "message", "Consistency check initiated");
        } catch (Exception ex) {
            logger.warning("Error occurred while initiating consistency check", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(textCommandService, command, res);
    }

    /**
     * Clears the WAN queues for the wan replication name and publisher ID defined
     * by the command parameters.
     *
     * @param command the HTTP command
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but
     *                                      named character encoding is not supported
     */
    private void handleWanClearQueues(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        final String[] params = decodeParams(command, 2);
        final String wanRepName = params[0];
        final String publisherId = params[1];
        try {
            textCommandService.getNode().getNodeEngine().getWanReplicationService().clearQueues(wanRepName, publisherId);
            res = response(RestUtil.ResponseType.SUCCESS, "message", "WAN replication queues are cleared.");
        } catch (Exception ex) {
            logger.warning("Error occurred while clearing queues", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(textCommandService, command, res);
    }

    /**
     * Broadcasts a new {@link WanReplicationConfig} to all members. The config is defined
     * by an encoded JSON as a first parameter of the HTTP command.
     *
     * @param command the HTTP command
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but
     *                                      named character encoding is not supported
     */
    private void handleAddWanConfig(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        final String[] params = decodeParams(command, 1);
        final String wanConfigJson = params[0];
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
            res = response(RestUtil.ResponseType.SUCCESS, "message", "WAN configuration added.");
        } catch (Exception ex) {
            logger.warning("Error occurred while adding WAN config", ex);
            res = exceptionResponse(ex);
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    /**
     * Pauses a WAN publisher on this member only. The publisher is identified
     * by the WAN replication name and publisher ID passed as parameters to
     * the HTTP command.
     *
     * @param command the HTTP command
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but
     *                                      named character encoding is not supported
     * @see com.hazelcast.config.WanPublisherState#PAUSED
     */
    private void handleWanPausePublisher(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        String[] params = decodeParams(command, 2);
        String wanReplicationName = params[0];
        String publisherId = params[1];
        WanReplicationService service = textCommandService.getNode().getNodeEngine().getWanReplicationService();

        try {
            service.pause(wanReplicationName, publisherId);
            res = response(RestUtil.ResponseType.SUCCESS, "message", "WAN publisher paused");
        } catch (Exception ex) {
            logger.warning("Error occurred while pausing WAN publisher", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(textCommandService, command, res);
    }

    /**
     * Stops a WAN publisher on this member only. The publisher is identified
     * by the WAN replication name and publisher ID passed as parameters to
     * the HTTP command.
     *
     * @param command the HTTP command
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but
     *                                      named character encoding is not supported
     * @see com.hazelcast.config.WanPublisherState#STOPPED
     */
    private void handleWanStopPublisher(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        String[] params = decodeParams(command, 2);
        String wanReplicationName = params[0];
        String publisherId = params[1];
        WanReplicationService service = textCommandService.getNode().getNodeEngine().getWanReplicationService();

        try {
            service.stop(wanReplicationName, publisherId);
            res = response(RestUtil.ResponseType.SUCCESS, "message", "WAN publisher stopped");
        } catch (Exception ex) {
            logger.warning("Error occurred while stopping WAN publisher", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(textCommandService, command, res);
    }

    /**
     * Resumes a WAN publisher on this member only. The publisher is identified
     * by the WAN replication name and publisher ID passed as parameters to
     * the HTTP command.
     *
     * @param command the HTTP command
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but
     *                                      named character encoding is not supported
     * @see com.hazelcast.config.WanPublisherState#REPLICATING
     */
    private void handleWanResumePublisher(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        String[] params = decodeParams(command, 2);
        String wanReplicationName = params[0];
        String publisherId = params[1];
        WanReplicationService service = textCommandService.getNode().getNodeEngine().getWanReplicationService();

        try {
            service.resume(wanReplicationName, publisherId);
            res = response(RestUtil.ResponseType.SUCCESS, "message", "WAN publisher resumed");
        } catch (Exception ex) {
            logger.warning("Error occurred while resuming WAN publisher", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(textCommandService, command, res);
    }

    private void handleUpdatePermissions(HttpPostCommand command) {
        String res = response(RestUtil.ResponseType.FORBIDDEN);
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
        return;
    }

    // This is intentionally not used. Instead handleUpdatePermissions() returns FORBIDDEN always.
    private void doHandleUpdatePermissions(HttpPostCommand command) throws UnsupportedEncodingException {
        SecurityService securityService = textCommandService.getNode().getSecurityService();
        if (securityService == null) {
            String res = response(RestUtil.ResponseType.FAIL,
                    "message", "Security features are only available on Hazelcast Enterprise!");
            command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
            return;
        }

        String res;
        byte[] data = command.getData();
        String[] strList = bytesToString(data).split("&");
        //Start from 3rd item of strList as first two are used for credentials
        String permConfigsJSON = URLDecoder.decode(strList[2], "UTF-8");

        try {
            UpdatePermissionConfigRequest request = new UpdatePermissionConfigRequest();
            request.fromJson(Json.parse(permConfigsJSON).asObject());
            securityService.refreshClientPermissions(request.getPermissionConfigs());
            res = response(RestUtil.ResponseType.SUCCESS, "message", "Permissions updated.");
        } catch (Exception ex) {
            logger.warning("Error occurred while updating permission config", ex);
            res = exceptionResponse(ex);
        }

        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    @Override
    public void handleRejection(HttpPostCommand command) {
        handle(command);
    }
}
