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
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.dto.WanReplicationConfigDTO;
import com.hazelcast.internal.management.operation.SetLicenseOperation;
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
import static com.hazelcast.internal.ascii.rest.HttpPostCommandProcessor.ResponseType.FAIL;
import static com.hazelcast.internal.ascii.rest.HttpPostCommandProcessor.ResponseType.SUCCESS;
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
                return;
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
                command.send403();
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

    private void handleChangeClusterState(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while changing cluster state",
                withAuthentication(command -> {
                    byte[] data = command.getData();
                    String[] strList = bytesToString(data).split("&");
                    Node node = textCommandService.getNode();
                    ClusterService clusterService = node.getClusterService();
                    String stateParam = URLDecoder.decode(strList[2], "UTF-8");
                    ClusterState state = ClusterState.valueOf(upperCaseInternal(stateParam));
                    if (!state.equals(clusterService.getClusterState())) {
                        clusterService.changeClusterState(state);
                        JsonObject res = response(SUCCESS,
                                "state", state.toString().toLowerCase(StringUtil.LOCALE_INTERNAL));
                        prepareResponse(command, res);
                    } else {
                        JsonObject res = response(FAIL,
                                "state", state.toString().toLowerCase(StringUtil.LOCALE_INTERNAL));
                        prepareResponse(command, res);
                    }
                })).handle(cmd);
    }

    private void handleGetClusterState(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while getting cluster state",
                withAuthentication(command -> {
                    Node node = textCommandService.getNode();
                    ClusterService clusterService = node.getClusterService();
                    ClusterState clusterState = clusterService.getClusterState();
                    prepareResponse(command, response(SUCCESS, "state", lowerCaseInternal(clusterState.toString())));
                })).handle(cmd);
    }

    private void handleChangeClusterVersion(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while changing cluster version",
                withAuthentication(command -> {
                    byte[] data = command.getData();
                    String[] strList = bytesToString(data).split("&");
                    Node node = textCommandService.getNode();
                    ClusterService clusterService = node.getClusterService();
                    String versionParam = URLDecoder.decode(strList[2], "UTF-8");
                    Version version = Version.of(versionParam);
                    clusterService.changeClusterVersion(version);
                    JsonObject rsp = response(SUCCESS, "version", clusterService.getClusterVersion().toString());
                    prepareResponse(command, rsp);
                })).handle(cmd);
    }

    private void handleForceStart(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while handling force start",
                withAuthentication(command -> {
                    Node node = textCommandService.getNode();
                    boolean success = node.getNodeExtension().getInternalHotRestartService().triggerForceStart();
                    prepareResponse(command, response(success ? SUCCESS : FAIL));
                })).handle(cmd);
    }

    private void handlePartialStart(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while handling partial start",
                withAuthentication(command -> {
                    Node node = textCommandService.getNode();
                    boolean success = node.getNodeExtension().getInternalHotRestartService().triggerPartialStart();
                    prepareResponse(command, response(success ? SUCCESS : FAIL));
                })).handle(cmd);
    }

    private void handleHotRestartBackup(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while invoking hot backup",
                withAuthentication(command -> {
                    textCommandService.getNode().getNodeExtension().getHotRestartService().backup();
                    prepareResponse(command, response(SUCCESS));
                })).handle(cmd);
    }

    private void handleHotRestartBackupInterrupt(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while interrupting hot backup",
                withAuthentication(command -> {
                    textCommandService.getNode().getNodeExtension().getHotRestartService().interruptBackupTask();
                    prepareResponse(command, response(SUCCESS));
                })).handle(cmd);
    }

    private void handleClusterShutdown(HttpPostCommand command) {
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            if (!checkCredentials(command)) {
                command.send403();
                textCommandService.sendResponse(command);
            } else {
                sendResponse(command, response(ResponseType.SUCCESS));
                clusterService.shutdown();
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while shutting down cluster", throwable);
            sendResponse(command, exceptionResponse(throwable));
        }
    }

    private void handleListNodes(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while listing nodes",
                withAuthentication(command -> {
                    Node node = textCommandService.getNode();
                    ClusterService clusterService = node.getClusterService();
                    final String responseTxt = clusterService.getMembers().toString() + "\n"
                            + node.getBuildInfo().getVersion() + "\n"
                            + System.getProperty("java.version");
                    prepareResponse(command, response(SUCCESS, "response", responseTxt));
                })).handle(cmd);
    }

    private void handleShutdownNode(HttpPostCommand command) {
        try {
            Node node = textCommandService.getNode();
            if (!checkCredentials(command)) {
                command.send403();
                textCommandService.sendResponse(command);
            } else {
                sendResponse(command, response(ResponseType.SUCCESS));
                node.hazelcastInstance.shutdown();
            }
        } catch (Throwable throwable) {
            logger.warning("Error occurred while shutting down", throwable);
            sendResponse(command, exceptionResponse(throwable));
        }
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
     * @param cmd the HTTP command
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private void handleWanSyncMap(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while syncing map",
                withAuthentication(command -> {
                    String[] params = decodeParams(command, 5);
                    String wanRepName = params[2];
                    String publisherId = params[3];
                    String mapName = params[4];
                    UUID uuid = textCommandService.getNode().getNodeEngine().getWanReplicationService()
                                                  .syncMap(wanRepName, publisherId, mapName);
                    prepareResponse(command, response(SUCCESS, "message", "Sync initiated", "uuid", uuid.toString()));
                })).handle(cmd);
    }

    /**
     * Initiates WAN sync for all maps and the wan replication name and publisher ID
     * defined
     * by the command parameters.
     *
     * @param cmd the HTTP command
     */
    private void handleWanSyncAllMaps(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while syncing maps",
                withAuthentication(command -> {
                    final String[] params = decodeParams(command, 4);
                    final String wanRepName = params[2];
                    final String publisherId = params[3];
                    UUID uuid = textCommandService.getNode().getNodeEngine().getWanReplicationService()
                                                  .syncAllMaps(wanRepName, publisherId);
                    prepareResponse(command, response(SUCCESS, "message", "Sync initiated", "uuid", uuid.toString()));
                })).handle(cmd);
    }

    /**
     * Initiates a WAN consistency check for a single map and the WAN replication
     * name and publisher ID defined by the command parameters.
     *
     * @param cmd the HTTP command
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private void handleWanConsistencyCheck(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while syncing maps",
                withAuthentication(command -> {
                    String[] params = decodeParams(command, 5);
                    String wanReplicationName = params[2];
                    String publisherId = params[3];
                    String mapName = params[4];
                    WanReplicationService service = textCommandService.getNode()
                                                                      .getNodeEngine().getWanReplicationService();
                    UUID uuid = service.consistencyCheck(wanReplicationName, publisherId, mapName);
                    prepareResponse(command, response(SUCCESS,
                            "message", "Consistency check initiated", "uuid", uuid.toString()));
                })).handle(cmd);
    }

    /**
     * Clears the WAN queues for the wan replication name and publisher ID defined
     * by the command parameters.
     *
     * @param cmd the HTTP command
     */
    private void handleWanClearQueues(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while clearing queues",
                withAuthentication(command -> {
                    final String[] params = decodeParams(command, 4);
                    final String wanRepName = params[2];
                    final String publisherId = params[3];
                    textCommandService.getNode().getNodeEngine()
                                      .getWanReplicationService()
                                      .removeWanEvents(wanRepName, publisherId);
                    prepareResponse(command, response(SUCCESS, "message", "WAN replication queues are cleared."));
                })).handle(cmd);
    }

    /**
     * Broadcasts a new {@link WanReplicationConfig} to all members. The config is defined
     * by an encoded JSON as a first parameter of the HTTP command.
     *
     * @param cmd the HTTP command
     */
    private void handleAddWanConfig(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while adding WAN config",
                withAuthentication(command -> {
                    JsonObject res;
                    String[] params = decodeParams(command, 3);
                    String wanConfigJson = params[2];
                    WanReplicationConfigDTO dto = new WanReplicationConfigDTO(new WanReplicationConfig());
                    dto.fromJson(Json.parse(wanConfigJson).asObject());

                    AddWanConfigResult result = textCommandService.getNode().getNodeEngine()
                                                                  .getWanReplicationService()
                                                                  .addWanReplicationConfig(dto.getConfig());
                    res = response(SUCCESS, "message", "WAN configuration added.");
                    res.add("addedPublisherIds", Json.array(result.getAddedPublisherIds().toArray(new String[]{})));
                    res.add("ignoredPublisherIds", Json.array(result.getIgnoredPublisherIds().toArray(new String[]{})));
                    prepareResponse(command, res);
                })).handle(cmd);
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
    private void handleWanPausePublisher(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while pausing WAN publisher",
                withAuthentication(command -> {
                    String[] params = decodeParams(command, 4);
                    String wanReplicationName = params[2];
                    String publisherId = params[3];
                    WanReplicationService service = textCommandService.getNode().getNodeEngine()
                                                                      .getWanReplicationService();
                    service.pause(wanReplicationName, publisherId);
                    prepareResponse(command, response(SUCCESS, "message", "WAN publisher paused"));
                })).handle(cmd);
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
    private void handleWanStopPublisher(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while stopping WAN publisher",
                withAuthentication(command -> {
                    String[] params = decodeParams(command, 4);
                    String wanReplicationName = params[2];
                    String publisherId = params[3];
                    WanReplicationService service = textCommandService.getNode().getNodeEngine()
                                                                      .getWanReplicationService();
                    service.stop(wanReplicationName, publisherId);
                    prepareResponse(command, response(SUCCESS, "message", "WAN publisher stopped"));
                })).handle(cmd);
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
    private void handleWanResumePublisher(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while stopping WAN publisher",
                withAuthentication(command -> {
                    String[] params = decodeParams(command, 4);
                    String wanReplicationName = params[2];
                    String publisherId = params[3];
                    WanReplicationService service = textCommandService.getNode().getNodeEngine()
                                                                      .getWanReplicationService();
                    service.resume(wanReplicationName, publisherId);
                    prepareResponse(command, response(SUCCESS, "message", "WAN publisher resumed"));
                })).handle(cmd);
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

    private static JsonObject exceptionResponse(Throwable throwable) {
        return response(FAIL, "message", throwable.getMessage());
    }

    protected static JsonObject response(ResponseType type, String... attributes) {
        JsonObject object = new JsonObject()
                .add("status", type.toString());
        if (attributes.length > 0) {
            for (int i = 0; i < attributes.length; ) {
                String key = attributes[i++];
                String value = attributes[i++];
                if (value != null) {
                    object.add(key, value);
                }
            }
        }
        return object;
    }

    protected enum ResponseType {
        SUCCESS, FAIL;

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
    private boolean authenticate(HttpPostCommand command, String userName, String pass)
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

    protected void sendResponse(HttpPostCommand command, JsonObject json) {
        prepareResponse(command, json);
        textCommandService.sendResponse(command);
    }

    @Override
    public void handleRejection(HttpPostCommand command) {
        handle(command);
    }

    private void handleSetLicense(HttpPostCommand cmd) {
        withExceptionHandling("Error occurred while updating the license",
                withAuthentication(command -> {
                    try {
                        final int retryCount = 100;
                        byte[] data = command.getData();
                        String[] strList = bytesToString(data).split("&");
                        // assumes that both groupName and password are present
                        String licenseKey = strList.length > 2 ? URLDecoder.decode(strList[2], "UTF-8") : null;
                        invokeOnStableClusterSerial(textCommandService.getNode().nodeEngine,
                                () -> new SetLicenseOperation(licenseKey), retryCount).get();
                        prepareResponse(command, responseOnSetLicenseSuccess());
                    } catch (ExecutionException executionException) {
                        logger.warning("Error occurred while updating the license", executionException.getCause());
                        prepareResponse(command, exceptionResponse(executionException.getCause()));
                    }
                })).handle(cmd);
    }

    private ExceptionThrowingCommandHandler withAuthentication(ExceptionThrowingCommandHandler commandHandler) {
        return command -> {
            if (checkCredentials(command)) {
                commandHandler.handle(command);
            } else {
                command.send403();
            }
        };
    }

    private CommandHandler withExceptionHandling(String errorMsg,
                                                 ExceptionThrowingCommandHandler commandHandler) {
        return cmd -> {
            try {
                commandHandler.handle(cmd);
            } catch (Throwable throwable) {
                logger.warning(errorMsg, throwable);
                prepareResponse(cmd, exceptionResponse(throwable));
            }
        };
    }

    private interface CommandHandler {
        void handle(HttpPostCommand command);
    }

    private interface ExceptionThrowingCommandHandler {
        void handle(HttpPostCommand command) throws Throwable;
    }

    protected JsonObject responseOnSetLicenseSuccess() {
        return response(SUCCESS);
    }

}
