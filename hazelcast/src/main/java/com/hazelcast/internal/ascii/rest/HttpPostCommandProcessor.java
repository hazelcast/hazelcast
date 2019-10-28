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

package com.hazelcast.internal.ascii.rest;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.dto.WanReplicationConfigDTO;
import com.hazelcast.internal.management.operation.SetLicenseOperation;
import com.hazelcast.internal.util.JsonUtil;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.wan.impl.AddWanConfigResult;
import com.hazelcast.wan.impl.WanReplicationService;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterSerial;
import static com.hazelcast.internal.util.StringUtil.bytesToString;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;

@SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:methodcount", "checkstyle:methodlength"})
public class HttpPostCommandProcessor extends HttpCommandProcessor<HttpPostCommand> {
    private static final byte[] QUEUE_SIMPLE_VALUE_CONTENT_TYPE = stringToBytes("text/plain");
    private final ILogger logger;


    public HttpPostCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
        this.logger = textCommandService.getNode().getLogger(HttpPostCommandProcessor.class);
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public void handle(HttpPostCommand command) {
        boolean sendResponse = true;
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
            } else {
                command.send404();
            }
        } catch (IndexOutOfBoundsException e) {
            command.send400();
        } catch (Exception e) {
            command.send500();
        }
        if (sendResponse) {
            textCommandService.sendResponse(command);
        }
    }

    private void handleChangeClusterState(HttpPostCommand command) {
        byte[] data = command.getData();
        String[] strList = bytesToString(data).split("&");
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            if (authenticate(command, strList[0], strList.length > 1 ? strList[1] : null)) {
                String stateParam = URLDecoder.decode(strList[2], "UTF-8");
                ClusterState state = ClusterState.valueOf(upperCaseInternal(stateParam));
                if (!state.equals(clusterService.getClusterState())) {
                    clusterService.changeClusterState(state);
                    res = response(ResponseType.SUCCESS, "state", state.toString().toLowerCase(StringUtil.LOCALE_INTERNAL));
                } else {
                    res = response(ResponseType.FAIL, "state", state.toString().toLowerCase(StringUtil.LOCALE_INTERNAL));
                }
            } else {
                res = response(ResponseType.FORBIDDEN);
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while changing cluster state", throwable);
            res = exceptionResponse(throwable);
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    private void handleGetClusterState(HttpPostCommand command) {
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            if (!checkCredentials(command)) {
                res = response(ResponseType.FORBIDDEN);
            } else {
                ClusterState clusterState = clusterService.getClusterState();
                res = response(ResponseType.SUCCESS, "state", lowerCaseInternal(clusterState.toString()));
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
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            if (authenticate(command, strList[0], strList.length > 1 ? strList[1] : null)) {
                String versionParam = URLDecoder.decode(strList[2], "UTF-8");
                Version version = Version.of(versionParam);
                clusterService.changeClusterVersion(version);
                res = response(ResponseType.SUCCESS, "version", clusterService.getClusterVersion().toString());
            } else {
                res = response(ResponseType.FORBIDDEN);
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while changing cluster version", throwable);
            res = exceptionResponse(throwable);
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    private void handleForceStart(HttpPostCommand command) {
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

    private void handlePartialStart(HttpPostCommand command) {
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

    private void handleHotRestartBackup(HttpPostCommand command) {
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

    private void handleHotRestartBackupInterrupt(HttpPostCommand command) {
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

    private void handleClusterShutdown(HttpPostCommand command) {
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

    private void handleListNodes(HttpPostCommand command) {
        String res;
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            if (!checkCredentials(command)) {
                res = response(ResponseType.FORBIDDEN);
            } else {
                final String responseTxt = clusterService.getMembers().toString() + "\n"
                        + node.getBuildInfo().getVersion() + "\n"
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

    private void handleShutdownNode(HttpPostCommand command) {
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
            suffix = uri.substring(URI_QUEUES.length());
        }
        int indexSlash = suffix.lastIndexOf('/');

        String queueName;
        if (indexSlash == -1) {
            queueName = suffix;
        } else {
            queueName = suffix.substring(0, indexSlash);
            simpleValue = suffix.substring(indexSlash + 1);
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
        byte[] res;
        String[] strList = bytesToString(command.getData()).split("&");
        if (authenticate(command, strList[0], strList.length > 1 ? strList[1] : null)) {
            ManagementCenterService managementCenterService = textCommandService.getNode().getManagementCenterService();
            if (managementCenterService != null) {
                String url = URLDecoder.decode(strList[2], "UTF-8");
                res = managementCenterService.clusterWideUpdateManagementCenterUrl(url);
            } else {
                logger.warning(
                        "Unable to change URL of ManagementCenter as the ManagementCenterService is not running on this member.");
                res = HttpCommand.RES_204;
            }
        } else {
            res = HttpCommand.RES_403;
        }

        command.setResponse(res);
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
            UUID uuid = textCommandService.getNode().getNodeEngine().getWanReplicationService()
                                          .syncMap(wanRepName, publisherId, mapName);
            res = response(ResponseType.SUCCESS, "message", "Sync initiated", "uuid", uuid.toString());
        } catch (Exception ex) {
            logger.warning("Error occurred while syncing map", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(command, res);
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
            UUID uuid = textCommandService.getNode().getNodeEngine().getWanReplicationService()
                                          .syncAllMaps(wanRepName, publisherId);
            res = response(ResponseType.SUCCESS, "message", "Sync initiated", "uuid", uuid.toString());
        } catch (Exception ex) {
            logger.warning("Error occurred while syncing maps", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(command, res);
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
            UUID uuid = service.consistencyCheck(wanReplicationName, publisherId, mapName);
            res = response(ResponseType.SUCCESS, "message", "Consistency check initiated", "uuid", uuid.toString());
        } catch (Exception ex) {
            logger.warning("Error occurred while initiating consistency check", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(command, res);
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
            textCommandService.getNode().getNodeEngine().getWanReplicationService().removeWanEvents(wanRepName, publisherId);
            res = response(ResponseType.SUCCESS, "message", "WAN replication queues are cleared.");
        } catch (Exception ex) {
            logger.warning("Error occurred while clearing queues", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(command, res);
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
        String[] params = decodeParams(command, 1);
        String wanConfigJson = params[0];
        try {
            WanReplicationConfigDTO dto = new WanReplicationConfigDTO(new WanReplicationConfig());
            dto.fromJson(Json.parse(wanConfigJson).asObject());

            AddWanConfigResult result = textCommandService.getNode().getNodeEngine()
                                                          .getWanReplicationService()
                                                          .addWanReplicationConfig(dto.getConfig());
            res = response(ResponseType.SUCCESS,
                    "message", "WAN configuration added.",
                    "addedPublisherIds", result.getAddedPublisherIds(),
                    "ignoredPublisherIds", result.getIgnoredPublisherIds());
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
     * @see WanPublisherState#PAUSED
     */
    private void handleWanPausePublisher(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        String[] params = decodeParams(command, 2);
        String wanReplicationName = params[0];
        String publisherId = params[1];
        WanReplicationService service = textCommandService.getNode().getNodeEngine().getWanReplicationService();

        try {
            service.pause(wanReplicationName, publisherId);
            res = response(ResponseType.SUCCESS, "message", "WAN publisher paused");
        } catch (Exception ex) {
            logger.warning("Error occurred while pausing WAN publisher", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(command, res);
    }

    /**
     * Stops a WAN publisher on this member only. The publisher is identified
     * by the WAN replication name and publisher ID passed as parameters to
     * the HTTP command.
     *
     * @param command the HTTP command
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but
     *                                      named character encoding is not supported
     * @see WanPublisherState#STOPPED
     */
    private void handleWanStopPublisher(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        String[] params = decodeParams(command, 2);
        String wanReplicationName = params[0];
        String publisherId = params[1];
        WanReplicationService service = textCommandService.getNode().getNodeEngine().getWanReplicationService();

        try {
            service.stop(wanReplicationName, publisherId);
            res = response(ResponseType.SUCCESS, "message", "WAN publisher stopped");
        } catch (Exception ex) {
            logger.warning("Error occurred while stopping WAN publisher", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(command, res);
    }

    /**
     * Resumes a WAN publisher on this member only. The publisher is identified
     * by the WAN replication name and publisher ID passed as parameters to
     * the HTTP command.
     *
     * @param command the HTTP command
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but
     *                                      named character encoding is not supported
     * @see WanPublisherState#REPLICATING
     */
    private void handleWanResumePublisher(HttpPostCommand command) throws UnsupportedEncodingException {
        String res;
        String[] params = decodeParams(command, 2);
        String wanReplicationName = params[0];
        String publisherId = params[1];
        WanReplicationService service = textCommandService.getNode().getNodeEngine().getWanReplicationService();

        try {
            service.resume(wanReplicationName, publisherId);
            res = response(ResponseType.SUCCESS, "message", "WAN publisher resumed");
        } catch (Exception ex) {
            logger.warning("Error occurred while resuming WAN publisher", ex);
            res = exceptionResponse(ex);
        }
        sendResponse(command, res);
    }

    private void handleUpdatePermissions(HttpPostCommand command) {
        String res = response(ResponseType.FORBIDDEN);
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    private void handleCPMember(final HttpPostCommand command) throws UnsupportedEncodingException {
        if (!checkCredentials(command)) {
            command.send403();
            textCommandService.sendResponse(command);
            return;
        }

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
        if (!checkCredentials(command)) {
            command.send403();
            textCommandService.sendResponse(command);
            return;
        }

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
        if (checkCredentials(command)) {
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
        } else {
            command.send403();
            textCommandService.sendResponse(command);
        }
    }

    private CPSubsystemManagementService getCpSubsystemManagementService() {
        return getCpSubsystem().getCPSubsystemManagementService();
    }

    private CPSubsystem getCpSubsystem() {
        return textCommandService.getNode().getNodeEngine().getHazelcastInstance().getCPSubsystem();
    }

    protected static String exceptionResponse(Throwable throwable) {
        return response(ResponseType.FAIL, "message", throwable.getMessage());
    }

    protected static String response(ResponseType type, Object... attributes) {
        final StringBuilder builder = new StringBuilder("{");
        builder.append("\"status\":\"").append(type).append("\"");
        if (attributes.length > 0) {
            for (int i = 0; i < attributes.length; ) {
                final String key = attributes[i++].toString();
                final Object value = attributes[i++];
                if (value != null) {
                    builder.append(String.format(",\"%s\":%s", key, JsonUtil.toJson(value)));
                }
            }
        }
        return builder.append("}").toString();
    }

    protected enum ResponseType {
        SUCCESS, FAIL, FORBIDDEN;

        @Override
        public String toString() {
            return super.toString().toLowerCase(StringUtil.LOCALE_INTERNAL);
        }
    }

    /**
     * Decodes HTTP post params contained in {@link HttpPostCommand#getData()}. The data
     * should be encoded in UTF-8 and joined together with an ampersand (&).
     *
     * @param command    the HTTP post command
     * @param paramCount the number of parameters expected in the command
     * @return the decoded params
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but
     *                                      named character encoding is not supported
     */
    private static String[] decodeParams(HttpPostCommand command, int paramCount) throws UnsupportedEncodingException {
        final byte[] data = command.getData();
        final String[] encoded = bytesToString(data).split("&");
        final String[] decoded = new String[encoded.length];
        for (int i = 0; i < paramCount; i++) {
            decoded[i] = URLDecoder.decode(encoded[i], "UTF-8");
        }
        return decoded;
    }

    private boolean checkCredentials(HttpPostCommand command) throws UnsupportedEncodingException {
        byte[] data = command.getData();
        if (data == null) {
            return false;
        }
        final String[] strList = bytesToString(data).split("&", -1);
        return authenticate(command, strList[0], strList.length > 1 ? strList[1] : null);
    }

    /**
     * Checks if the request is valid. If Hazelcast Security is not enabled, then only the given user name is compared to
     * cluster name in node configuration. Otherwise member JAAS authentication (member login module stack) is used to
     * authenticate the command.
     */
    protected boolean authenticate(HttpPostCommand command, String userName, String pass)
            throws UnsupportedEncodingException {
        String decodedName = userName != null ? URLDecoder.decode(userName, "UTF-8") : null;
        Node node = textCommandService.getNode();
        SecurityContext securityContext = node.getNodeExtension().getSecurityContext();
        String clusterName = node.getConfig().getClusterName();
        if (securityContext == null) {
            if (pass != null && !pass.isEmpty()) {
                logger.fine("Password was provided but the Hazelcast Security is disabled.");
            }
            return clusterName.equals(decodedName);
        }
        String decodedPass = pass != null ? URLDecoder.decode(pass, "UTF-8") : null;
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(decodedName, decodedPass);
        try {
            // we don't have an argument for clusterName in HTTP request, so let's reuse the "username" here
            LoginContext lc = securityContext.createMemberLoginContext(decodedName, credentials, command.getConnection());
            lc.login();
        } catch (LoginException e) {
            return false;
        }
        return true;
    }

    protected void sendResponse(HttpPostCommand command, String value) {
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(value));
        textCommandService.sendResponse(command);
    }

    @Override
    public void handleRejection(HttpPostCommand command) {
        handle(command);
    }

    private void handleSetLicense(HttpPostCommand command) {
        final int retryCount = 100;
        String res;
        byte[] data = command.getData();
        try {
            String[] strList = bytesToString(data).split("&");
            if (authenticate(command, strList[0], strList.length > 1 ? strList[1] : null)) {
                // assumes that both groupName and password are present
                String licenseKey = strList.length > 2 ? URLDecoder.decode(strList[2], "UTF-8") : null;
                invokeOnStableClusterSerial(textCommandService.getNode().nodeEngine, () -> new SetLicenseOperation(licenseKey),
                        retryCount).get();
                res = responseOnSetLicenseSuccess();
            } else {
                res = response(ResponseType.FORBIDDEN);
            }
        } catch (ExecutionException executionException) {
            logger.warning("Error occurred while updating the license", executionException.getCause());
            res = exceptionResponse(executionException.getCause());
        } catch (Throwable throwable) {
            logger.warning("Error occurred while updating the license", throwable);
            res = exceptionResponse(throwable);
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    protected String responseOnSetLicenseSuccess() {
        return response(ResponseType.SUCCESS);
    }

}
