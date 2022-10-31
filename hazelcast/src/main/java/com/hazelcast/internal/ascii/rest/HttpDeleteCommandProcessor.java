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

import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.util.StringUtil;

import static com.hazelcast.internal.ascii.rest.RestCallExecution.ObjectType.MAP;
import static com.hazelcast.internal.ascii.rest.RestCallExecution.ObjectType.QUEUE;

public class HttpDeleteCommandProcessor extends HttpCommandProcessor<HttpDeleteCommand> {

    public HttpDeleteCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService, textCommandService.getNode().getLogger(HttpPostCommandProcessor.class));
    }

    @Override
    public void handle(HttpDeleteCommand command) {
        try {
            String uri = command.getURI();
            if (uri.startsWith(URI_MAPS)) {
                handleMap(command, uri);
            } else if (uri.startsWith(URI_QUEUES)) {
                handleQueue(command, uri);
            } else {
                command.send404();
            }
        } catch (IndexOutOfBoundsException e) {
            command.send400();
        } catch (Exception e) {
            command.send500();
        }

        textCommandService.sendResponse(command);
    }

    private void handleMap(HttpDeleteCommand command, String uri) {
        command.getExecutionDetails().setObjectType(MAP);
        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        if (indexEnd == -1) {
            String mapName = uri.substring(URI_MAPS.length());
            textCommandService.deleteAll(mapName);
            command.getExecutionDetails().setObjectName(mapName);
            command.send200();
        } else {
            String mapName = uri.substring(URI_MAPS.length(), indexEnd);
            command.getExecutionDetails().setObjectName(mapName);
            String key = uri.substring(indexEnd + 1);
            key = StringUtil.stripTrailingSlash(key);
            textCommandService.delete(mapName, key);
            command.send200();
        }
    }

    private void handleQueue(HttpDeleteCommand command, String uri) {
        // Poll an item from the default queue in 3 seconds
        // http://127.0.0.1:5701/hazelcast/rest/queues/default/3
        int indexEnd = uri.indexOf('/', URI_QUEUES.length());
        command.getExecutionDetails().setObjectType(QUEUE);
        String queueName = uri.substring(URI_QUEUES.length(), indexEnd);
        command.getExecutionDetails().setObjectName(queueName);
        String secondStr = (uri.length() > (indexEnd + 1)) ? uri.substring(indexEnd + 1) : null;
        int seconds = (secondStr == null) ? 0 : Integer.parseInt(secondStr);
        Object value = textCommandService.poll(queueName, seconds);
        if (value == null) {
            command.send204();
        } else {
            Object responseValue;
            if (value instanceof byte[] || value instanceof RestValue || value instanceof String) {
                responseValue = value;
            } else {
                responseValue = textCommandService.toByteArray(value);
            }
            prepareResponse(command, responseValue);
        }
    }

    @Override
    public void handleRejection(HttpDeleteCommand command) {
        handle(command);
    }
}
