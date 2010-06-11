/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl.ascii.rest;

import com.hazelcast.impl.ascii.TextCommandService;

public class HttpPostCommandProcessor extends HttpCommandProcessor<HttpPostCommand> {

    public HttpPostCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    public void handle(HttpPostCommand command) {
        String uri = command.getURI();
        if (uri.startsWith(URI_MAPS)) {
            int indexEnd = uri.indexOf('/', URI_MAPS.length());
            String mapName = uri.substring(URI_MAPS.length(), indexEnd);
            String key = uri.substring(indexEnd + 1);
            byte[] data = command.getData();
            System.out.println("data is " + data + " data length " + data.length);
            textCommandService.put(mapName, key, new RestValue(data, command.getContentType()), 0);
            command.setResponse(HttpCommand.RES_204);
        } else {
            command.setResponse(HttpCommand.RES_400);
        }
        textCommandService.sendResponse(command);
    }

    public void handleRejection(HttpPostCommand command) {
        handle(command);
    }
}