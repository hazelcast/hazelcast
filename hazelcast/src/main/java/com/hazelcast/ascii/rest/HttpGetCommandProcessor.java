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
import com.hazelcast.instance.Node;
import com.hazelcast.nio.ConnectionManager;

import static com.hazelcast.util.StringUtil.stringToBytes;

public class HttpGetCommandProcessor extends HttpCommandProcessor<HttpGetCommand> {

    public static final String QUEUE_SIZE_COMMAND = "size";

    public HttpGetCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    public void handle(HttpGetCommand command) {
        String uri = command.getURI();
        if (uri.startsWith(URI_MAPS)) {
            int indexEnd = uri.indexOf('/', URI_MAPS.length());
            String mapName = uri.substring(URI_MAPS.length(), indexEnd);
            String key = uri.substring(indexEnd + 1);
            Object value = textCommandService.get(mapName, key);
            prepareResponse(command, value);
        } else if (uri.startsWith(URI_QUEUES)) {
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

        } else if (uri.startsWith(URI_CLUSTER)) {
            Node node = textCommandService.getNode();
            StringBuilder res = new StringBuilder(node.getClusterService().membersString());
            res.append("\n");
            ConnectionManager connectionManager = node.getConnectionManager();
            res.append("ConnectionCount: ").append(connectionManager.getCurrentClientConnections());
            res.append("\n");
            res.append("AllConnectionCount: ").append(connectionManager.getAllTextConnections());
            res.append("\n");
            command.setResponse(null, stringToBytes(res.toString()));
        } else {
            command.send400();
        }
        textCommandService.sendResponse(command);
    }

    public void handleRejection(HttpGetCommand command) {
        handle(command);
    }

    private void prepareResponse(HttpGetCommand command, Object value) {
        if (value == null) {
            command.send204();
        } else {
            if (value instanceof byte[]) {
                command.setResponse(HttpCommand.CONTENT_TYPE_BINARY, (byte[]) value);
            } else if (value instanceof RestValue) {
                RestValue restValue = (RestValue) value;
                command.setResponse(restValue.getContentType(), restValue.getValue());
            } else if (value instanceof String) {
                command.setResponse(HttpCommand.CONTENT_TYPE_PLAIN_TEXT, stringToBytes((String) value));
            } else {
                command.setResponse(HttpCommand.CONTENT_TYPE_BINARY, textCommandService.toByteArray(value));
            }
        }
    }
}
