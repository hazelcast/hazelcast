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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.cp.session.CPSession;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.nio.AggregateEndpointManager;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.internal.nio.NetworkingService;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.StringUtil;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.internal.ascii.TextCommandConstants.MIME_TEXT_PLAIN;
import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_BINARY;
import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_JSON;
import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_PLAIN_TEXT;
import static com.hazelcast.internal.ascii.rest.HttpCommand.RES_200_WITH_NO_CONTENT;
import static com.hazelcast.internal.ascii.rest.HttpCommand.RES_503;
import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;

@SuppressWarnings({"checkstyle:methodcount"})
public class HttpGetCommandProcessor extends HttpCommandProcessor<HttpGetCommand> {

    public static final String QUEUE_SIZE_COMMAND = "size";

    private static final String HEALTH_PATH_PARAM_NODE_STATE = "/node-state";
    private static final String HEALTH_PATH_PARAM_CLUSTER_STATE = "/cluster-state";
    private static final String HEALTH_PATH_PARAM_CLUSTER_SAFE = "/cluster-safe";
    private static final String HEALTH_PATH_PARAM_MIGRATION_QUEUE_SIZE = "/migration-queue-size";
    private static final String HEALTH_PATH_PARAM_CLUSTER_SIZE = "/cluster-size";

    public HttpGetCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity"})
    public void handle(HttpGetCommand command) {
        boolean sendResponse = true;
        try {
            String uri = command.getURI();
            if (uri.startsWith(URI_MAPS)) {
                handleMap(command, uri);
            } else if (uri.startsWith(URI_QUEUES)) {
                handleQueue(command, uri);
            } else if (uri.startsWith(URI_INSTANCE)) {
                handleInstance(command);
            } else if (uri.startsWith(URI_CLUSTER)) {
                handleCluster(command);
            } else if (uri.startsWith(URI_HEALTH_READY)) {
                handleHealthReady(command);
            } else if (uri.startsWith(URI_HEALTH_URL)) {
                handleHealthcheck(command, uri);
            } else if (uri.startsWith(URI_CLUSTER_VERSION_URL)) {
                handleGetClusterVersion(command);
            } else if (uri.startsWith(URI_LICENSE_INFO)) {
                handleLicense(command);
            } else if (uri.startsWith(URI_CP_GROUPS_URL)) {
                handleCPGroupRequest(command);
                sendResponse = false;
            } else if (uri.startsWith(URI_LOCAL_CP_MEMBER_URL)) {
                // this else if block must be above get-cp-members block
                handleGetLocalCPMember(command);
            } else if (uri.startsWith(URI_CP_MEMBERS_URL)) {
                handleGetCPMembers(command);
                sendResponse = false;
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

    private void handleHealthReady(HttpGetCommand command) {
        Node node = textCommandService.getNode();

        if (node.isRunning()
                && node.getNodeExtension().isStartCompleted()) {
            command.setResponse(RES_200_WITH_NO_CONTENT);
        } else {
            command.setResponse(RES_503);
        }
    }

    private void handleHealthcheck(HttpGetCommand command, String uri) {
        Node node = textCommandService.getNode();
        NodeState nodeState = node.getState();

        ClusterServiceImpl clusterService = node.getClusterService();
        ClusterState clusterState = clusterService.getClusterState();
        int clusterSize = clusterService.getMembers().size();

        InternalPartitionService partitionService = node.getPartitionService();
        boolean memberStateSafe = partitionService.isMemberStateSafe();
        boolean clusterSafe = memberStateSafe && !partitionService.hasOnGoingMigration();
        long migrationQueueSize = partitionService.getMigrationQueueSize();

        String healthParameter = uri.substring(URI_HEALTH_URL.length());
        if (healthParameter.equals(HEALTH_PATH_PARAM_NODE_STATE)) {
            if (NodeState.SHUT_DOWN.equals(nodeState)) {
                command.setResponse(RES_503);
            } else {
                command.setResponse(null, stringToBytes(nodeState.toString()));
            }
        } else if (healthParameter.equals(HEALTH_PATH_PARAM_CLUSTER_STATE)) {
            command.setResponse(null, stringToBytes(clusterState.toString()));
        } else if (healthParameter.equals(HEALTH_PATH_PARAM_CLUSTER_SAFE)) {
            if (clusterSafe) {
                command.send200();
            } else {
                command.setResponse(RES_503);
            }
        } else if (healthParameter.equals(HEALTH_PATH_PARAM_MIGRATION_QUEUE_SIZE)) {
            command.setResponse(null, stringToBytes(Long.toString(migrationQueueSize)));
        } else if (healthParameter.equals(HEALTH_PATH_PARAM_CLUSTER_SIZE)) {
            command.setResponse(null, stringToBytes(Integer.toString(clusterSize)));
        } else if (healthParameter.isEmpty()) {
            StringBuilder res = new StringBuilder();
            res.append("Hazelcast::NodeState=").append(nodeState).append("\n");
            res.append("Hazelcast::ClusterState=").append(clusterState).append("\n");
            res.append("Hazelcast::ClusterSafe=").append(booleanToString(clusterSafe)).append("\n");
            res.append("Hazelcast::MigrationQueueSize=").append(migrationQueueSize).append("\n");
            res.append("Hazelcast::ClusterSize=").append(clusterSize).append("\n");
            command.setResponse(MIME_TEXT_PLAIN, stringToBytes(res.toString()));
        } else {
            command.send400();
        }
    }

    private static String booleanToString(boolean b) {
        return Boolean.toString(b).toUpperCase(StringUtil.LOCALE_INTERNAL);
    }

    private void handleGetClusterVersion(HttpGetCommand command) {
        String res = "{\"status\":\"${STATUS}\",\"version\":\"${VERSION}\"}";
        Node node = textCommandService.getNode();
        ClusterService clusterService = node.getClusterService();
        res = res.replace("${STATUS}", "success");
        res = res.replace("${VERSION}", clusterService.getClusterVersion().toString());
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    private void handleCPGroupRequest(HttpGetCommand command) {
        String uri = command.getURI();
        if (uri.contains(URI_CP_SESSIONS_SUFFIX)) {
            handleGetCPSessions(command);
        } else if (uri.endsWith(URI_CP_GROUPS_URL) || uri.endsWith(URI_CP_GROUPS_URL + "/")) {
            handleGetCPGroupIds(command);
        } else {
            handleGetCPGroupByName(command);
        }
    }

    private void handleGetCPGroupIds(final HttpGetCommand command) {
        CompletionStage<Collection<CPGroupId>> f = getCpSubsystemManagementService().getCPGroupIds();
        f.whenCompleteAsync((groupIds, t) -> {
            if (t == null) {
                JsonArray arr = new JsonArray();
                for (CPGroupId groupId : groupIds) {
                    arr.add(toJson(groupId));
                }
                command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(arr.toString()));
                textCommandService.sendResponse(command);
            } else {
                command.send500();
                textCommandService.sendResponse(command);
            }
        });
    }

    private void handleGetCPSessions(final HttpGetCommand command) {
        String uri = command.getURI();
        String prefix = URI_CP_GROUPS_URL + "/";
        int i = uri.indexOf(URI_CP_SESSIONS_SUFFIX);
        String groupName = uri.substring(prefix.length(), i).trim();
        getCpSubsystem().getCPSessionManagementService()
                        .getAllSessions(groupName)
                        .whenCompleteAsync((sessions, t) -> {
                            if (t == null) {
                                JsonArray sessionsArr = new JsonArray();
                                for (CPSession session : sessions) {
                                    sessionsArr.add(toJson(session));
                                }

                                command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(sessionsArr.toString()));
                                textCommandService.sendResponse(command);
                            } else {
                                if (peel(t) instanceof IllegalArgumentException) {
                                    command.send404();
                                } else {
                                    command.send500();
                                }

                                textCommandService.sendResponse(command);
                            }
                        });
    }

    private void handleGetCPGroupByName(final HttpGetCommand command) {
        String prefix = URI_CP_GROUPS_URL + "/";
        String groupName = command.getURI().substring(prefix.length()).trim();
        CompletionStage<CPGroup> f = getCpSubsystemManagementService().getCPGroup(groupName);
        f.whenCompleteAsync((group, t) -> {
            if (t == null) {
                if (group != null) {
                    JsonObject json = new JsonObject();
                    json.add("id", toJson(group.id()))
                        .add("status", group.status().name());

                    JsonArray membersArr = new JsonArray();
                    for (CPMember member : group.members()) {
                        membersArr.add(toJson(member));
                    }

                    json.add("members", membersArr);

                    command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(json.toString()));
                } else {
                    command.send404();
                }

                textCommandService.sendResponse(command);
            } else {
                command.send500();
                textCommandService.sendResponse(command);
            }
        });
    }

    private void handleGetCPMembers(final HttpGetCommand command) {
        CompletionStage<Collection<CPMember>> f = getCpSubsystemManagementService().getCPMembers();
        f.whenCompleteAsync((cpMembers, t) -> {
            if (t == null) {
                JsonArray arr = new JsonArray();
                for (CPMember cpMember : cpMembers) {
                    arr.add(toJson(cpMember));
                }

                command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(arr.toString()));
                textCommandService.sendResponse(command);
            } else {
                command.send500();
                textCommandService.sendResponse(command);
            }
        });
    }

    private void handleGetLocalCPMember(final HttpGetCommand command) {
        CPMember localCPMember = getCpSubsystem().getLocalCPMember();
        if (localCPMember != null) {
            command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(toJson(localCPMember).toString()));
        } else {
            command.send404();
        }
    }

    private CPSubsystemManagementService getCpSubsystemManagementService() {
        return getCpSubsystem().getCPSubsystemManagementService();
    }

    private CPSubsystem getCpSubsystem() {
        return textCommandService.getNode().getNodeEngine().getHazelcastInstance().getCPSubsystem();
    }

    private JsonObject toJson(CPGroupId groupId) {
        return new JsonObject().add("name", groupId.getName()).add("id", groupId.getId());
    }

    private JsonObject toJson(CPMember cpMember) {
        Address address = cpMember.getAddress();
        return new JsonObject()
                .add("uuid", cpMember.getUuid().toString())
                .add("address", "[" + address.getHost() + "]:" + address.getPort());
    }

    private JsonObject toJson(CPSession cpSession) {
        Address address = cpSession.endpoint();
        return new JsonObject()
                .add("id", cpSession.id())
                .add("creationTime", cpSession.creationTime())
                .add("expirationTime", cpSession.expirationTime())
                .add("version", cpSession.version())
                .add("endpoint", "[" + address.getHost() + "]:" + address.getPort())
                .add("endpointType", cpSession.endpointType().name())
                .add("endpointName", cpSession.endpointName());
    }

    /**
     * Sets the HTTP response to a string containing basic cluster information:
     * <ul>
     * <li>Member list</li>
     * <li>Client connection count</li>
     * <li>Connection count</li>
     * </ul>
     *
     * @param command the HTTP request
     */
    private void handleCluster(HttpGetCommand command) {
        Node node = textCommandService.getNode();
        StringBuilder res = new StringBuilder(node.getClusterService().getMemberListString());
        res.append("\n");
        NetworkingService ns = node.getNetworkingService();
        EndpointManager cem = ns.getEndpointManager(CLIENT);
        AggregateEndpointManager aem = ns.getAggregateEndpointManager();
        res.append("ConnectionCount: ").append(cem.getActiveConnections().size());
        res.append("\n");
        res.append("AllConnectionCount: ").append(aem.getActiveConnections().size());
        res.append("\n");
        command.setResponse(null, stringToBytes(res.toString()));
    }

    /**
     * Sets the HTTP response to a string containing basic instance information in JSON format:
     * <ul>
     * <li>Instance name</li>
     * </ul>
     *
     * @param command the HTTP request
     */
    private void handleInstance(HttpGetCommand command) {
        JsonObject jsonResponse = new JsonObject().add("name", textCommandService.getInstanceName());
        command.setResponse(CONTENT_TYPE_JSON, stringToBytes(jsonResponse.toString()));
    }

    private void handleQueue(HttpGetCommand command, String uri) {
        int indexEnd = uri.indexOf('/', URI_QUEUES.length());
        String queueName = uri.substring(URI_QUEUES.length(), indexEnd);
        String secondStr = (uri.length() > (indexEnd + 1)) ? uri.substring(indexEnd + 1) : null;

        if (QUEUE_SIZE_COMMAND.equalsIgnoreCase(secondStr)) {
            int size = textCommandService.size(queueName);
            prepareResponse(command, Integer.toString(size));
        } else {
            int seconds = (secondStr == null) ? 0 : Integer.parseInt(secondStr);
            Object value = textCommandService.poll(queueName, seconds);
            prepareResponse(command, value);
        }
    }

    private void handleMap(HttpGetCommand command, String uri) {
        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        String mapName = uri.substring(URI_MAPS.length(), indexEnd);
        String key = uri.substring(indexEnd + 1);
        Object value = textCommandService.get(mapName, key);
        prepareResponse(command, value);
    }

    @Override
    public void handleRejection(HttpGetCommand command) {
        handle(command);
    }

    private void prepareResponse(HttpGetCommand command, Object value) {
        if (value == null) {
            command.send204();
        } else if (value instanceof byte[]) {
            command.setResponse(CONTENT_TYPE_BINARY, (byte[]) value);
        } else if (value instanceof RestValue) {
            RestValue restValue = (RestValue) value;
            command.setResponse(restValue.getContentType(), restValue.getValue());
        } else if (value instanceof HazelcastJsonValue) {
            command.setResponse(CONTENT_TYPE_JSON, stringToBytes(value.toString()));
        } else if (value instanceof String) {
            command.setResponse(CONTENT_TYPE_PLAIN_TEXT, stringToBytes((String) value));
        } else {
            command.setResponse(CONTENT_TYPE_BINARY, textCommandService.toByteArray(value));
        }
    }

    /**
     * License info is mplemented in the Enterprise GET command processor. The OS version returns simple "404 Not Found".
     */
    protected void handleLicense(HttpGetCommand command) {
        command.send404();
    }
}
