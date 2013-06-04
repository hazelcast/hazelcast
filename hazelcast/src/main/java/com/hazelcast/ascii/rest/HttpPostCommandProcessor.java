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

package com.hazelcast.ascii.rest;

import com.hazelcast.ascii.TextCommandService;
import com.hazelcast.management.ManagementCenterService;

import java.net.URLDecoder;

public class HttpPostCommandProcessor extends HttpCommandProcessor<HttpPostCommand> {

    private static final byte[] QUEUE_SIMPLE_VALUE_CONTENT_TYPE = "text/plain".getBytes();

    public HttpPostCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    public void handle(HttpPostCommand command) {
        try {
            String uri = command.getURI();
            if (uri.startsWith(URI_MAPS)) {
                int indexEnd = uri.indexOf('/', URI_MAPS.length());
                String mapName = uri.substring(URI_MAPS.length(), indexEnd);
                String key = uri.substring(indexEnd + 1);
                byte[] data = command.getData();
                textCommandService.put(mapName, key, new RestValue(data, command.getContentType()), 0);
                command.setResponse(HttpCommand.RES_204);
            } else if (uri.startsWith(URI_MANCENTER_CHANGE_URL)) {
                if (textCommandService.getNode().getGroupProperties().MC_URL_CHANGE_ENABLED.getBoolean()) {
                    byte[] res = HttpCommand.RES_204;
                    byte[] data = command.getData();
                    String[] strList = new String(data).split("&");
                    String cluster = URLDecoder.decode(strList[0], "UTF-8");
                    String pass = URLDecoder.decode(strList[1], "UTF-8");
                    String url = URLDecoder.decode(strList[2], "UTF-8");

                    ManagementCenterService managementCenterService = textCommandService.getNode().getManagementCenterService();
                    if (managementCenterService != null) {
                        res = managementCenterService.changeWebServerUrlOverCluster(cluster, pass, url);
                    }
                    command.setResponse(res);
                }
                else {
                    command.setResponse(HttpCommand.RES_503);
                }
            } else if (uri.startsWith(URI_QUEUES)) {
                String queueName = null;
                String simpleValue = null;
                String suffix;
                if (uri.endsWith("/")) {
                    suffix = uri.substring(URI_QUEUES.length(), uri.length() - 1);
                } else {
                    suffix = uri.substring(URI_QUEUES.length(), uri.length());
                }
                int indexSlash = suffix.lastIndexOf("/");
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
                    data = simpleValue.getBytes();
                    contentType = QUEUE_SIMPLE_VALUE_CONTENT_TYPE;
                }
                boolean offerResult = textCommandService.offer(queueName, new RestValue(data, contentType));
                if (offerResult) {
                    command.setResponse(HttpCommand.RES_204);
                } else {
                    command.setResponse(HttpCommand.RES_503);
                }
            } else {
                command.setResponse(HttpCommand.RES_400);
            }
        } catch (Exception e) {
            command.setResponse(HttpCommand.RES_500);
        }
        textCommandService.sendResponse(command);
    }

    public void handleRejection(HttpPostCommand command) {
        handle(command);
    }
}
