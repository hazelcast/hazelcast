/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.ascii.memcache;

import com.hazelcast.internal.ascii.TextCommandConstants;
import com.hazelcast.internal.ascii.TextCommandService;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class DeleteCommandProcessor extends MemcacheCommandProcessor<DeleteCommand> {

    public DeleteCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    @Override
    public void handle(DeleteCommand command) {
        String key;
        key = URLDecoder.decode(command.getKey(), StandardCharsets.UTF_8);
        String mapName = DEFAULT_MAP_NAME;
        int index = key.indexOf(':');
        if (index != -1) {
            mapName = MAP_NAME_PREFIX + key.substring(0, index);
            key = key.substring(index + 1);
        }
        if (key.equals("")) {
            textCommandService.deleteAll(mapName);
        } else {
            final Object oldValue = textCommandService.delete(mapName, key);
            if (oldValue == null) {
                textCommandService.incrementDeleteMissCount();
                command.setResponse(TextCommandConstants.NOT_FOUND);
            } else {
                textCommandService.incrementDeleteHitCount(1);
                command.setResponse(TextCommandConstants.DELETED);
            }
        }

        if (command.shouldReply()) {
            textCommandService.sendResponse(command);
        }
    }

    @Override
    public void handleRejection(DeleteCommand command) {
        handle(command);
    }
}
