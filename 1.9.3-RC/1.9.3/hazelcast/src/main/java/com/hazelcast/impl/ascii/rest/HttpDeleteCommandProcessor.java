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

public class HttpDeleteCommandProcessor extends HttpCommandProcessor<HttpDeleteCommand> {

    public HttpDeleteCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    public void handle(HttpDeleteCommand command) {
        String uri = command.getURI();
        if (uri.startsWith(URI_MAPS)) {
            int indexEnd = uri.indexOf('/', URI_MAPS.length());
            String mapName = uri.substring(URI_MAPS.length(), indexEnd);
            String key = uri.substring(indexEnd + 1);
            Object value = textCommandService.delete(mapName, key);
            command.send204();
        } else {
            command.send400();
        }
        textCommandService.sendResponse(command);
    }

    public void handleRejection(HttpDeleteCommand command) {
        handle(command);
    }
}