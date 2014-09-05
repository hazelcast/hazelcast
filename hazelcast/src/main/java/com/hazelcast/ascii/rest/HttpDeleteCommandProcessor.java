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

import static com.hazelcast.util.StringUtil.stringToBytes;

public class HttpDeleteCommandProcessor extends HttpCommandProcessor<HttpDeleteCommand> {

    public HttpDeleteCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    public void handle(HttpDeleteCommand command) {
        String uri = command.getURI();
        if (uri.startsWith(URI_MAPS)) {
            int indexEnd = uri.indexOf('/', URI_MAPS.length());
            if (indexEnd == -1) {
                String mapName = uri.substring(URI_MAPS.length(), uri.length());
                textCommandService.deleteAll(mapName);
                command.send200();

            } else {
                String mapName = uri.substring(URI_MAPS.length(), indexEnd);
                String key = uri.substring(indexEnd + 1);
                textCommandService.delete(mapName, key);
                command.send200();
            }
        } else if (uri.startsWith(URI_QUEUES)) {
            // Poll an item from the default queue in 3 seconds
            // http://127.0.0.1:5701/hazelcast/rest/queues/default/3
            int indexEnd = uri.indexOf('/', URI_QUEUES.length());
            String queueName = uri.substring(URI_QUEUES.length(), indexEnd);
            String secondStr = (uri.length() > (indexEnd + 1)) ? uri.substring(indexEnd + 1) : null;
            int seconds = (secondStr == null) ? 0 : Integer.parseInt(secondStr);
            Object value = textCommandService.poll(queueName, seconds);
            if (value == null) {
                command.send204();
            } else {
                if (value instanceof byte[]) {
                    command.setResponse(null, (byte[]) value);
                } else if (value instanceof RestValue) {
                    RestValue restValue = (RestValue) value;
                    command.setResponse(restValue.getContentType(), restValue.getValue());
                } else if (value instanceof String) {
                    command.setResponse(HttpCommand.CONTENT_TYPE_PLAIN_TEXT, stringToBytes((String) value));
                } else {
                    command.setResponse(null, textCommandService.toByteArray(value));
                }
            }
        } else {
            command.send400();
        }
        textCommandService.sendResponse(command);
    }

    public void handleRejection(HttpDeleteCommand command) {
        handle(command);
    }
}
