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

package com.hazelcast.internal.ascii.rest;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.dto.WanReplicationConfigDTO;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.impl.LoggingServiceImpl;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.wan.impl.AddWanConfigResult;
import com.hazelcast.wan.impl.WanReplicationService;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_JSON;
import static com.hazelcast.internal.ascii.rest.HttpCommandProcessor.ResponseType.FAIL;
import static com.hazelcast.internal.ascii.rest.HttpCommandProcessor.ResponseType.SUCCESS;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_200;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_400;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_403;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_500;
import static com.hazelcast.internal.ascii.rest.RestCallExecution.ObjectType.MAP;
import static com.hazelcast.internal.ascii.rest.RestCallExecution.ObjectType.QUEUE;
import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;

@SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:methodcount", "checkstyle:methodlength"})
public class HttpPostCommandProcessor extends HttpCommandProcessor<HttpPostCommand> {
    private static final byte[] QUEUE_SIMPLE_VALUE_CONTENT_TYPE = stringToBytes("text/plain");

    public HttpPostCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService, textCommandService.getNode().getLogger(HttpPostCommandProcessor.class));
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public void handle(HttpPostCommand command) {
        boolean sendResponse = true;
        try {
            String uri = command.getURI();
            if (uri.startsWith(URI_MAPS)) {
                handleMap(command, uri);
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
            } else if (uri.startsWith(URI_PERSISTENCE_BACKUP_INTERRUPT_CLUSTER_URL)) {
                handleBackupInterrupt(command, false);
            } else if (uri.startsWith(URI_HOT_RESTART_BACKUP_INTERRUPT_CLUSTER_URL)) {
                handleBackupInterrupt(command, true);
            } else if (uri.startsWith(URI_PERSISTENCE_BACKUP_CLUSTER_URL)) {
                handleBackup(command, false);
            } else if (uri.startsWith(URI_HOT_RESTART_BACKUP_CLUSTER_URL)) {
                handleBackup(command, true);
            } else if (uri.startsWith(URI_PARTIALSTART_CLUSTER_URL)) {
                handlePartialStart(command);
            } else if (uri.startsWith(URI_CLUSTER_NODES_URL)) {
                handleListNodes(command);
            } else if (uri.startsWith(URI_SHUTDOWN_NODE_CLUSTER_URL)) {
                handleShutdownNode(command);
                return;
            } else if (uri.startsWith(URI_WAN_SYNC_MAP)) {
                handleWanSyncMap(command);
            } else if (uri.startsWith(URI_WAN_SYNC_ALL_MAPS)) {
                handleWanSyncAllMaps(command);
            } else if (uri.startsWith(URI_WAN_CLEAR_QUEUES)) {
                handleWanClearQueues(command);
            } else if (uri.startsWith(URI_ADD_WAN_CONFIG)) {
                handleAddWanConfig(command);
            } else if (uri.startsWith(URI_WAN_PAUSE_PUBLISHER)) {
                handleWanPausePublisher(command);
            } else if (uri.startsWith(URI_WAN_STOP_PUBLISHER)) {
                handleWanStopPublisher(command);
            } else if (uri.startsWith(URI_WAN_RESUME_PUBLISHER)) {
                handleWanResumePublisher(command);
            } else if (uri.startsWith(URI_WAN_CONSISTENCY_CHECK_MAP)) {
                handleWanConsistencyCheck(command);
            } else if (uri.startsWith(URI_CP_MEMBERS_URL)) {
                handleCPMember(command);
                sendResponse = false;
            } else if (uri.startsWith(URI_CP_GROUPS_URL)) {
                handleCPGroup(command);
                sendResponse = false;
            } else if (uri.startsWith(URI_RESET_CP_SUBSYSTEM_URL)) {
                handleResetCPSubsystem(command);
                sendResponse = false;
            } else if (uri.startsWith(URI_LICENSE_INFO)) {
                handleSetLicense(command);
            } else if (uri.startsWith(URI_LOG_LEVEL_RESET)) {
                handleLogLevelReset(command);
            } else if (uri.startsWith(URI_LOG_LEVEL)) {
                handleLogLevelSet(command);
            } else if (uri.startsWith(URI_CONFIG_RELOAD)) {
                handleConfigReload(command);
            } else if (uri.startsWith(URI_CONFIG_UPDATE)) {
                handleConfigUpdate(command);
            } else {
                command.send404();
            }
        } catch (HttpBadRequestException e) {
            prepareResponse(SC_400, command, response(FAIL, "message", e.getMessage()));
            sendResponse = true;
        } catch (HttpForbiddenException e) {
            prepareResponse(SC_403, command, response(FAIL, "message", "unauthenticated"));
            sendResponse = true;
        } catch (Throwable e) {
            logger.warning("An error occurred while handling request " + command, e);
            prepareResponse(SC_500, command, exceptionResponse(e));
        }

        if (sendResponse) {
            textCommandService.sendResponse(command);
        }
    }

    private void handleChangeClusterState(HttpPostCommand cmd) throws Throwable {
        String[] params = decodeParamsAndAuthenticate(cmd, 3);
        ClusterService clusterService = getNode().getClusterService();
        ClusterState state = ClusterState.valueOf(upperCaseInternal(params[2]));
        if (!state.equals(clusterService.getClusterState())) {
            clusterService.changeClusterState(state);
            JsonObject res = response(SUCCESS,
                    "state", state.toString().toLowerCase(StringUtil.LOCALE_INTERNAL));
            prepareResponse(cmd, res);
        } else {
            JsonObject res = response(FAIL,
                    "state", state.toString().toLowerCase(StringUtil.LOCALE_INTERNAL));
            prepareResponse(cmd, res);
        }
    }

    private void handleGetClusterState(HttpPostCommand cmd) throws Throwable {
        decodeParamsAndAuthenticate(cmd, 2);
        ClusterService clusterService = getNode().getClusterService();
        ClusterState clusterState = clusterService.getClusterState();
        prepareResponse(cmd, response(SUCCESS, "state", lowerCaseInternal(clusterState.toString())));
    }

    private void handleChangeClusterVersion(HttpPostCommand cmd) throws Throwable {
        String[] params = decodeParamsAndAuthenticate(cmd, 3);
        ClusterService clusterService = getNode().getClusterService();
        Version version = Version.of(params[2]);
        clusterService.changeClusterVersion(version);
        JsonObject rsp = response(SUCCESS, "version", clusterService.getClusterVersion().toString());
        prepareResponse(cmd, rsp);
    }

    private void handleForceStart(HttpPostCommand cmd) throws Throwable {
        decodeParamsAndAuthenticate(cmd, 2);
        boolean success = getNode().getNodeExtension().getInternalHotRestartService().triggerForceStart();
        prepareResponse(cmd, response(success ? SUCCESS : FAIL));
    }

    private void handlePartialStart(HttpPostCommand cmd) throws Throwable {
        decodeParamsAndAuthenticate(cmd, 2);
        boolean success = getNode().getNodeExtension().getInternalHotRestartService().triggerPartialStart();
        prepareResponse(cmd, response(success ? SUCCESS : FAIL));
    }

    private void handleBackup(HttpPostCommand cmd, boolean deprecated) throws Throwable {
        decodeParamsAndAuthenticate(cmd, 2);
        getNode().getNodeExtension().getHotRestartService().backup();
        if (deprecated) {
            Map<String, Object> headers = new HashMap<>();
            headers.put("Content-Type", "application/json");
            headers.put("Deprecation", "true");
            headers.put("Warning", "299 - \"Deprecated API\". Please use /hazelcast/rest/management/cluster/backup"
                    + " instead. This API will be removed in future releases.");
            cmd.setResponseWithHeaders(SC_200, headers, stringToBytes(response(SUCCESS).toString()));
        } else {
            prepareResponse(cmd, response(SUCCESS));
        }
    }

    private void handleBackupInterrupt(HttpPostCommand cmd, boolean deprecated) throws Throwable {
        decodeParamsAndAuthenticate(cmd, 2);
        getNode().getNodeExtension().getHotRestartService().interruptBackupTask();
        if (deprecated) {
            Map<String, Object> headers = new HashMap<>();
            headers.put("Content-Type", CONTENT_TYPE_JSON);
            headers.put("Deprecation", "true");
            headers.put("Warning", "299 - \"Deprecated API\". Please use"
                    + " /hazelcast/rest/management/cluster/backupInterrupt instead. This API will be removed"
                    + " in future releases.");
            cmd.setResponseWithHeaders(SC_200, headers, stringToBytes(response(SUCCESS).toString()));
        } else {
            prepareResponse(cmd, response(SUCCESS));
        }
    }

    private void handleClusterShutdown(HttpPostCommand command) throws UnsupportedEncodingException {
        decodeParamsAndAuthenticate(command, 2);
        ClusterService clusterService = getNode().getClusterService();
        sendResponse(command, response(SUCCESS));
        clusterService.shutdown();
    }

    private void handleListNodes(HttpPostCommand cmd) throws Throwable {
        decodeParamsAndAuthenticate(cmd, 2);
        ClusterService clusterService = getNode().getClusterService();
        final String responseTxt = clusterService.getMembers().toString() + "\n"
                + getNode().getBuildInfo().getVersion() + "\n"
                + System.getProperty("java.version");
        prepareResponse(cmd, response(SUCCESS, "response", responseTxt));
    }

    private void handleShutdownNode(HttpPostCommand command) throws UnsupportedEncodingException {
        decodeParamsAndAuthenticate(command, 2);
        sendResponse(command, response(SUCCESS));
        getNode().hazelcastInstance.shutdown();
    }

    private void handleQueue(HttpPostCommand command, String uri) {
        String simpleValue = null;
        String suffix;
        int baseUriLength = URI_QUEUES.length();
        command.getExecutionDetails().setObjectType(QUEUE);
        if (uri.endsWith("/")) {
            int requestedUriLength = uri.length();
            if (baseUriLength == requestedUriLength) {
                throw new HttpBadRequestException("Missing queue name");
            }
            suffix = uri.substring(baseUriLength, requestedUriLength - 1);
        } else {
            suffix = uri.substring(baseUriLength);
        }
        int indexSlash = suffix.lastIndexOf('/');

        String queueName;
        if (indexSlash == -1) {
            queueName = suffix;
        } else {
            queueName = suffix.substring(0, indexSlash);
            simpleValue = suffix.substring(indexSlash + 1);
        }
        command.getExecutionDetails().setObjectName(queueName);
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
            command.send503();
        }
    }

    private void handleMap(HttpPostCommand command, String uri) {
        command.getExecutionDetails().setObjectType(MAP);
        uri = StringUtil.stripTrailingSlash(uri);
        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        if (indexEnd == -1) {
            throw new HttpBadRequestException("Missing map name");
        }
        String mapName = uri.substring(URI_MAPS.length(), indexEnd);
        command.getExecutionDetails().setObjectName(mapName);
        String key = uri.substring(indexEnd + 1);
        byte[] data = command.getData();
        textCommandService.put(mapName, key, new RestValue(data, command.getContentType()), -1);
        command.send200();
    }

    /**
     * Initiates a WAN sync for a single map and the wan replication name and publisher ID defined
     * by the command parameters.
     *
     * @param cmd the HTTP command
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private void handleWanSyncMap(HttpPostCommand cmd) throws Throwable {
        String[] params = decodeParamsAndAuthenticate(cmd, 5);

        String wanRepName = params[2];
        String publisherId = params[3];
        String mapName = params[4];
        UUID uuid = getNode().getNodeEngine().getWanReplicationService()
                .syncMap(wanRepName, publisherId, mapName);
        prepareResponse(cmd, response(SUCCESS, "message", "Sync initiated", "uuid", uuid.toString()));
    }

    /**
     * Initiates WAN sync for all maps and the wan replication name and publisher ID
     * defined
     * by the command parameters.
     *
     * @param cmd the HTTP command
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private void handleWanSyncAllMaps(HttpPostCommand cmd) throws Throwable {
        String[] params = decodeParamsAndAuthenticate(cmd, 4);

        final String wanRepName = params[2];
        final String publisherId = params[3];
        UUID uuid = getNode().getNodeEngine().getWanReplicationService()
                .syncAllMaps(wanRepName, publisherId);
        prepareResponse(cmd, response(SUCCESS, "message", "Sync initiated", "uuid", uuid.toString()));
    }

    /**
     * Initiates a WAN consistency check for a single map and the WAN replication
     * name and publisher ID defined by the command parameters.
     *
     * @param cmd the HTTP command
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private void handleWanConsistencyCheck(HttpPostCommand cmd) throws Throwable {
        String[] params = decodeParamsAndAuthenticate(cmd, 5);

        String wanReplicationName = params[2];
        String publisherId = params[3];
        String mapName = params[4];
        WanReplicationService service = getNode().getNodeEngine().getWanReplicationService();
        UUID uuid = service.consistencyCheck(wanReplicationName, publisherId, mapName);
        prepareResponse(cmd, response(SUCCESS,
                "message", "Consistency check initiated", "uuid", uuid.toString()));
    }

    /**
     * Clears the WAN queues for the wan replication name and publisher ID defined
     * by the command parameters.
     *
     * @param cmd the HTTP command
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private void handleWanClearQueues(HttpPostCommand cmd) throws Throwable {
        String[] params = decodeParamsAndAuthenticate(cmd, 4);

        final String wanRepName = params[2];
        final String publisherId = params[3];
        getNode().getNodeEngine().getWanReplicationService().removeWanEvents(wanRepName, publisherId);
        prepareResponse(cmd, response(SUCCESS, "message", "WAN replication queues are cleared."));
    }

    /**
     * Broadcasts a new {@link WanReplicationConfig} to all members. The config is defined
     * by an encoded JSON as a first parameter of the HTTP command.
     *
     * @param cmd the HTTP command
     */
    private void handleAddWanConfig(HttpPostCommand cmd) throws Throwable {
        String[] params = decodeParamsAndAuthenticate(cmd, 3);

        String wanConfigJson = params[2];
        WanReplicationConfigDTO dto = new WanReplicationConfigDTO(new WanReplicationConfig());
        dto.fromJson(Json.parse(wanConfigJson).asObject());

        AddWanConfigResult result = getNode().getNodeEngine()
                .getWanReplicationService()
                .addWanReplicationConfig(dto.getConfig());
        JsonObject res = response(SUCCESS, "message", "WAN configuration added.");
        res.add("addedPublisherIds", Json.array(result.getAddedPublisherIds().toArray(new String[]{})));
        res.add("ignoredPublisherIds", Json.array(result.getIgnoredPublisherIds().toArray(new String[]{})));
        prepareResponse(cmd, res);
    }

    /**
     * Pauses a WAN publisher on this member only. The publisher is identified
     * by the WAN replication name and publisher ID passed as parameters to
     * the HTTP command.
     *
     * @param cmd the HTTP command
     * @see WanPublisherState#PAUSED
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private void handleWanPausePublisher(HttpPostCommand cmd) throws Throwable {
        String[] params = decodeParamsAndAuthenticate(cmd, 4);

        String wanReplicationName = params[2];
        String publisherId = params[3];
        WanReplicationService service = getNode().getNodeEngine().getWanReplicationService();
        service.pause(wanReplicationName, publisherId);
        prepareResponse(cmd, response(SUCCESS, "message", "WAN publisher paused"));
    }

    /**
     * Stops a WAN publisher on this member only. The publisher is identified
     * by the WAN replication name and publisher ID passed as parameters to
     * the HTTP command.
     *
     * @param cmd the HTTP command
     * @see WanPublisherState#STOPPED
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private void handleWanStopPublisher(HttpPostCommand cmd) throws Throwable {
        String[] params = decodeParamsAndAuthenticate(cmd, 4);

        String wanReplicationName = params[2];
        String publisherId = params[3];
        WanReplicationService service = getNode().getNodeEngine().getWanReplicationService();
        service.stop(wanReplicationName, publisherId);
        prepareResponse(cmd, response(SUCCESS, "message", "WAN publisher stopped"));
    }

    /**
     * Resumes a WAN publisher on this member only. The publisher is identified
     * by the WAN replication name and publisher ID passed as parameters to
     * the HTTP command.
     *
     * @param cmd the HTTP command
     * @see WanPublisherState#REPLICATING
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private void handleWanResumePublisher(HttpPostCommand cmd) throws Throwable {
        String[] params = decodeParamsAndAuthenticate(cmd, 4);

        String wanReplicationName = params[2];
        String publisherId = params[3];
        WanReplicationService service = getNode().getNodeEngine().getWanReplicationService();
        service.resume(wanReplicationName, publisherId);
        prepareResponse(cmd, response(SUCCESS, "message", "WAN publisher resumed"));
    }

    private void handleCPMember(final HttpPostCommand command) throws UnsupportedEncodingException {
        decodeParamsAndAuthenticate(command, 2);
        String uri = command.getURI();
        if (uri.endsWith(URI_REMOVE_SUFFIX) || uri.endsWith(URI_REMOVE_SUFFIX + "/")) {
            handleRemoveCPMember(command);
        } else {
            handlePromoteToCPMember(command);
        }
    }

    private void handlePromoteToCPMember(final HttpPostCommand command) {
        if (getCpSubsystem().getLocalCPMember() != null) {
            command.send200();
            textCommandService.sendResponse(command);
            return;
        }

        getCpSubsystemManagementService().promoteToCPMember()
                .whenCompleteAsync((response, t) -> {
                    if (t == null) {
                        command.send200();
                        textCommandService.sendResponse(command);
                    } else {
                        logger.warning("Error while promoting CP member.", t);
                        command.send500();
                        textCommandService.sendResponse(command);
                    }
                });
    }

    private void handleRemoveCPMember(final HttpPostCommand command) {
        String uri = command.getURI();
        String prefix = URI_CP_MEMBERS_URL + "/";
        final UUID cpMemberUid = UUID.fromString(uri.substring(prefix.length(), uri.indexOf('/', prefix.length())).trim());
        getCpSubsystem().getCPSubsystemManagementService()
                .removeCPMember(cpMemberUid)
                .whenCompleteAsync((respone, t) -> {
                    if (t == null) {
                        command.send200();
                        textCommandService.sendResponse(command);
                    } else {
                        logger.warning("Error while removing CP member " + cpMemberUid, t);
                        if (peel(t) instanceof IllegalArgumentException) {
                            command.send400();
                        } else {
                            command.send500();
                        }

                        textCommandService.sendResponse(command);
                    }
                });
    }

    private void handleCPGroup(HttpPostCommand command) throws UnsupportedEncodingException {
        decodeParamsAndAuthenticate(command, 2);
        String uri = command.getURI();
        if (!uri.endsWith(URI_REMOVE_SUFFIX) && !uri.endsWith(URI_REMOVE_SUFFIX + "/")) {
            command.send404();
            textCommandService.sendResponse(command);
            return;
        }

        if (uri.contains(URI_CP_SESSIONS_SUFFIX)) {
            handleForceCloseCPSession(command);
        } else {
            handleForceDestroyCPGroup(command);
        }
    }

    private void handleForceCloseCPSession(final HttpPostCommand command) {
        String uri = command.getURI();
        String prefix = URI_CP_GROUPS_URL + "/";
        String suffix = URI_CP_SESSIONS_SUFFIX + "/";
        int i = uri.indexOf(suffix);
        String groupName = uri.substring(prefix.length(), i).trim();
        final long sessionId = Long.parseLong(uri.substring(i + suffix.length(), uri.indexOf('/', i + suffix.length())));

        getCpSubsystem().getCPSessionManagementService()
                .forceCloseSession(groupName, sessionId)
                .whenCompleteAsync((response, t) -> {
                    if (t == null) {
                        if (response) {
                            command.send200();
                        } else {
                            command.send400();
                        }
                        textCommandService.sendResponse(command);
                    } else {
                        logger.warning("Error while closing CP session", t);
                        command.send500();
                        textCommandService.sendResponse(command);
                    }
                });
    }

    private void handleForceDestroyCPGroup(final HttpPostCommand command) {
        String uri = command.getURI();
        String prefix = URI_CP_GROUPS_URL + "/";
        final String groupName = uri.substring(prefix.length(), uri.indexOf('/', prefix.length())).trim();
        if (METADATA_CP_GROUP_NAME.equals(groupName)) {
            command.send400();
            textCommandService.sendResponse(command);
            return;
        }

        getCpSubsystem().getCPSubsystemManagementService()
                .forceDestroyCPGroup(groupName)
                .whenCompleteAsync((response, t) -> {
                    if (t == null) {
                        command.send200();
                        textCommandService.sendResponse(command);
                    } else {
                        logger.warning("Error while destroying CP group " + groupName, t);
                        if (peel(t) instanceof IllegalArgumentException) {
                            command.send400();
                        } else {
                            command.send500();
                        }
                        textCommandService.sendResponse(command);
                    }
                });
    }

    private void handleResetCPSubsystem(final HttpPostCommand command) throws UnsupportedEncodingException {
        decodeParamsAndAuthenticate(command, 2);

        getCpSubsystem().getCPSubsystemManagementService()
                .reset()
                .whenCompleteAsync((response, t) -> {
                    if (t == null) {
                        command.send200();
                        textCommandService.sendResponse(command);
                    } else {
                        logger.warning("Error while resetting CP subsystem", t);
                        command.send500();
                        textCommandService.sendResponse(command);
                    }
                });
    }

    private CPSubsystemManagementService getCpSubsystemManagementService() {
        return getCpSubsystem().getCPSubsystemManagementService();
    }

    private CPSubsystem getCpSubsystem() {
        return getNode().getNodeEngine().getHazelcastInstance().getCPSubsystem();
    }

    @Override
    public void handleRejection(HttpPostCommand command) {
        handle(command);
    }

    protected void handleSetLicense(HttpPostCommand cmd) throws Throwable {
        // NO-OP in OS
        prepareResponse(cmd, response(SUCCESS));
    }

    private void handleLogLevelSet(HttpPostCommand command) throws UnsupportedEncodingException {
        String[] params = decodeParamsAndAuthenticate(command, 3);
        String level = params[2];

        LoggingServiceImpl loggingService = (LoggingServiceImpl) getNode().getLoggingService();
        loggingService.setLevel(level);
        prepareResponse(command, response(SUCCESS, "message", "log level is changed"));
    }

    private void handleLogLevelReset(HttpPostCommand command) throws UnsupportedEncodingException {
        decodeParamsAndAuthenticate(command, 2);

        LoggingServiceImpl loggingService = (LoggingServiceImpl) getNode().getLoggingService();
        loggingService.resetLevel();
        prepareResponse(command, response(SUCCESS, "message", "log level is reset"));
    }

    protected void handleConfigReload(HttpPostCommand command) throws UnsupportedEncodingException {
        prepareResponse(
                SC_500,
                command,
                response(FAIL, "message", "Configuration Reload requires Hazelcast Enterprise Edition")
        );
    }

    protected void handleConfigUpdate(HttpPostCommand command) throws UnsupportedEncodingException {
        prepareResponse(
                SC_500,
                command,
                response(FAIL, "message", "Configuration Update requires Hazelcast Enterprise Edition")
        );
    }
}
