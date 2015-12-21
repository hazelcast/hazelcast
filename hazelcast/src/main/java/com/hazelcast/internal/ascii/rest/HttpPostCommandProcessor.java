/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.management.ManagementCenterService;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;


import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.StringUtil.stringToBytes;

public class HttpPostCommandProcessor extends HttpCommandProcessor<HttpPostCommand> {

    private static final byte[] QUEUE_SIMPLE_VALUE_CONTENT_TYPE = stringToBytes("text/plain");

    public HttpPostCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    @Override
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
            } else if (uri.startsWith(URI_SHUTDOWN_CLUSTER_URL)) {
                handleClusterShutdown(command);
                return;
            } else if (uri.startsWith(URI_FORCESTART_CLUSTER_URL)) {
                handleForceStart(command);
            } else {
                command.setResponse(HttpCommand.RES_400);
            }
        } catch (Exception e) {
            command.setResponse(HttpCommand.RES_500);
        }
        textCommandService.sendResponse(command);
    }

    private void handleChangeClusterState(HttpPostCommand command) throws UnsupportedEncodingException  {
        byte[] data = command.getData();
        String[] strList = bytesToString(data).split("&");
        String groupName = URLDecoder.decode(strList[0], "UTF-8");
        String groupPass = URLDecoder.decode(strList[1], "UTF-8");
        String stateParam = URLDecoder.decode(strList[2], "UTF-8");
        String res = "{\"status\":\"${STATUS}\",\"state\":\"${STATE}\"}";
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            GroupConfig groupConfig = node.getConfig().getGroupConfig();
            if (!(groupConfig.getName().equals(groupName) && groupConfig.getPassword().equals(groupPass))) {
                res = res.replace("${STATUS}", "forbidden");
                res = res.replace("${STATE}", "null");
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
                    res = res.replace("${STATUS}", "success");
                    res = res.replace("${STATE}", state.toString().toLowerCase());
                } else {
                    res = res.replace("${STATUS}", "fail");
                    res = res.replace("${STATE}", state.toString().toLowerCase());
                }
            }
        } catch (Throwable throwable) {
            res = res.replace("${STATUS}", "fail");
            res = res.replace("${STATE}", "null");
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON , stringToBytes(res));
    }

    private void handleGetClusterState(HttpPostCommand command) throws UnsupportedEncodingException {
        byte[] data = command.getData();
        String[] strList = bytesToString(data).split("&");
        String groupName = URLDecoder.decode(strList[0], "UTF-8");
        String groupPass = URLDecoder.decode(strList[1], "UTF-8");
        String res = "{\"status\":\"${STATUS}\",\"state\":\"${STATE}\"}";
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            GroupConfig groupConfig = node.getConfig().getGroupConfig();
            if (!(groupConfig.getName().equals(groupName) && groupConfig.getPassword().equals(groupPass))) {
                res = res.replace("${STATUS}", "forbidden");
                res = res.replace("${STATE}", "null");
            } else {
                res = res.replace("${STATUS}", "success");
                res = res.replace("${STATE}", clusterService.getClusterState().toString().toLowerCase());
            }
        } catch (Throwable throwable) {
            res = res.replace("${STATUS}", "fail");
            res = res.replace("${STATE}", "null");
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON , stringToBytes(res));
    }

    private void handleForceStart(HttpPostCommand command) throws UnsupportedEncodingException {
        byte[] data = command.getData();
        String[] strList = bytesToString(data).split("&");
        String groupName = URLDecoder.decode(strList[0], "UTF-8");
        String groupPass = URLDecoder.decode(strList[1], "UTF-8");
        String res = "{\"status\":\"${STATUS}\"}";
        try {
            Node node = textCommandService.getNode();
            GroupConfig groupConfig = node.getConfig().getGroupConfig();
            if (!(groupConfig.getName().equals(groupName) && groupConfig.getPassword().equals(groupPass))) {
                res = res.replace("${STATUS}", "forbidden");
            } else {
                boolean success = node.getNodeExtension().triggerForceStart();
                res = res.replace("${STATUS}", success ? "success" : "fail");
            }
        } catch (Throwable throwable) {
            res = res.replace("${STATUS}", "fail");
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON , stringToBytes(res));
        textCommandService.sendResponse(command);
    }

    private void handleClusterShutdown(HttpPostCommand command)  throws UnsupportedEncodingException {
        byte[] data = command.getData();
        String[] strList = bytesToString(data).split("&");
        String groupName = URLDecoder.decode(strList[0], "UTF-8");
        String groupPass = URLDecoder.decode(strList[1], "UTF-8");
        String res = "{\"status\":\"${STATUS}\"}";
        try {
            Node node = textCommandService.getNode();
            ClusterService clusterService = node.getClusterService();
            GroupConfig groupConfig = node.getConfig().getGroupConfig();
            if (!(groupConfig.getName().equals(groupName) && groupConfig.getPassword().equals(groupPass))) {
                res = res.replace("${STATUS}", "forbidden");
            } else {
                res = res.replace("${STATUS}", "success");
                command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(res));
                textCommandService.sendResponse(command);
                clusterService.shutdown();
                return;
            }
        } catch (Throwable throwable) {
            res = res.replace("${STATUS}", "fail");
        }
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON , stringToBytes(res));
        textCommandService.sendResponse(command);
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
        if (textCommandService.getNode().getGroupProperties().getBoolean(GroupProperty.MC_URL_CHANGE_ENABLED)) {
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

    @Override
    public void handleRejection(HttpPostCommand command) {
        handle(command);
    }
}
