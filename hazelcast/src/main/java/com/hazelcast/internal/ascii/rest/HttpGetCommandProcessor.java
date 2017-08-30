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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.ConnectionManager;

import static com.hazelcast.internal.ascii.TextCommandConstants.MIME_TEXT_PLAIN;
import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_BINARY;
import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_PLAIN_TEXT;
import static com.hazelcast.util.StringUtil.stringToBytes;

public class HttpGetCommandProcessor extends HttpCommandProcessor<HttpGetCommand> {

    public static final String QUEUE_SIZE_COMMAND = "size";

    public HttpGetCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    @Override
    public void handle(HttpGetCommand command) {
        String uri = command.getURI();
        if (uri.startsWith(URI_MAPS)) {
            handleMap(command, uri);
        } else if (uri.startsWith(URI_QUEUES)) {
            handleQueue(command, uri);
        } else if (uri.startsWith(URI_CLUSTER)) {
            handleCluster(command);
        } else if (uri.equals(URI_HEALTH_URL)) {
            handleHealthcheck(command);
        } else if (uri.startsWith(URI_CLUSTER_VERSION_URL)) {
            handleGetClusterVersion(command);
        } else {
            command.send400();
        }
        textCommandService.sendResponse(command);
    }

    private void handleHealthcheck(HttpGetCommand command) {
        Node node = textCommandService.getNode();
        NodeState nodeState = node.getState();

        ClusterServiceImpl clusterService = node.getClusterService();
        ClusterState clusterState = clusterService.getClusterState();
        int clusterSize = clusterService.getMembers().size();

        InternalPartitionService partitionService = node.getPartitionService();
        boolean memberStateSafe = partitionService.isMemberStateSafe();
        boolean clusterSafe = memberStateSafe && !partitionService.hasOnGoingMigration();
        long migrationQueueSize = partitionService.getMigrationQueueSize();

        StringBuilder res = new StringBuilder();
        res.append("Hazelcast::NodeState=").append(nodeState).append("\n");
        res.append("Hazelcast::ClusterState=").append(clusterState).append("\n");
        res.append("Hazelcast::ClusterSafe=").append(Boolean.toString(clusterSafe).toUpperCase()).append("\n");
        res.append("Hazelcast::MigrationQueueSize=").append(migrationQueueSize).append("\n");
        res.append("Hazelcast::ClusterSize=").append(clusterSize).append("\n");
        command.setResponse(MIME_TEXT_PLAIN, stringToBytes(res.toString()));
    }

    private void handleGetClusterVersion(HttpGetCommand command) {
        String res = "{\"status\":\"${STATUS}\",\"version\":\"${VERSION}\"}";
        Node node = textCommandService.getNode();
        ClusterService clusterService = node.getClusterService();
        res = res.replace("${STATUS}", "success");
        res = res.replace("${VERSION}", clusterService.getClusterVersion().toString());
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
    }

    /**
     * Sets the HTTP response to a string containing basic cluster information:
     * <ul>
     *     <li>Member list</li>
     *     <li>Client connection count</li>
     *     <li>Connection count</li>
     * </ul>
     * @param command the HTTP request
     */
    private void handleCluster(HttpGetCommand command) {
        Node node = textCommandService.getNode();
        StringBuilder res = new StringBuilder(node.getClusterService().getMemberListString());
        res.append("\n");
        ConnectionManager connectionManager = node.getConnectionManager();
        res.append("ConnectionCount: ").append(connectionManager.getCurrentClientConnections());
        res.append("\n");
        res.append("AllConnectionCount: ").append(connectionManager.getAllTextConnections());
        res.append("\n");
        command.setResponse(null, stringToBytes(res.toString()));
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
        } else if (value instanceof String) {
            command.setResponse(CONTENT_TYPE_PLAIN_TEXT, stringToBytes((String) value));
        } else {
            command.setResponse(CONTENT_TYPE_BINARY, textCommandService.toByteArray(value));
        }
    }
}
