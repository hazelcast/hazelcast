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

public class HttpGetCommandProcessor extends HttpCommandProcessor<HttpGetCommand> {

    public HttpGetCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    public void handle(HttpGetCommand command) {
        String uri = command.getURI();
//            System.out.println("URI " + uri);
        if (uri.startsWith(URI_MAPS)) {
            int indexEnd = uri.indexOf('/', URI_MAPS.length());
            String mapName = uri.substring(URI_MAPS.length(), indexEnd);
//                System.out.println("mapName " + mapName);
            String key = uri.substring(indexEnd + 1);
            Object value = textCommandService.get(mapName, key);
            if (value == null) {
                command.send204();
            } else {
                if (value instanceof byte[]) {
                    command.setResponse(null, (byte[]) value);
                } else {
                    RestValue restValue = (RestValue) value;
                    command.setResponse(restValue.getContentType(), restValue.getValue());
                }
            }
        } else {
            command.send400();
        }
        textCommandService.sendResponse(command);
    }

    public void handleRejection(HttpGetCommand command) {
        handle(command);
    }
}