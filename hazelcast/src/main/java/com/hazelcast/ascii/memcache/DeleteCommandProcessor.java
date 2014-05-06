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

package com.hazelcast.ascii.memcache;

import com.hazelcast.ascii.TextCommandService;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static java.lang.String.format;

public class DeleteCommandProcessor extends MemcacheCommandProcessor<DeleteCommand> {

    public DeleteCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    public void handle(DeleteCommand command) {
        String key;
        try {
            key = URLDecoder.decode(command.getKey(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(format("failed to decode key [%s] using UTF-8", command.getKey()));
        }
        String mapName = DEFAULT_MAP_NAME;
        int index = key.indexOf(':');
        if (index != -1) {
            mapName = MAP_NAME_PRECEDER + key.substring(0, index);
            key = key.substring(index + 1);
        }
        if (key.equals("")) {
            textCommandService.deleteAll(mapName);
        } else {
            final Object oldValue = textCommandService.delete(mapName, key);
            if (oldValue == null) {
                textCommandService.incrementDeleteMissCount();
                command.setResponse(NOT_FOUND);
            } else {
                textCommandService.incrementDeleteHitCount(1);
                command.setResponse(DELETED);
            }
        }

        if (command.shouldReply()) {
            textCommandService.sendResponse(command);
        }
    }

    public void handleRejection(DeleteCommand command) {
        handle(command);
    }
}
