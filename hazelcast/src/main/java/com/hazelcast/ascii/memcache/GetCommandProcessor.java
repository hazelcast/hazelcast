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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;


import static com.hazelcast.util.StringUtil.stringToBytes;

public class GetCommandProcessor extends MemcacheCommandProcessor<GetCommand> {
    final boolean single;
    private final ILogger logger;

    public GetCommandProcessor(TextCommandService textCommandService, boolean single) {
        super(textCommandService);
        this.single = single;
        logger = textCommandService.getNode().getLogger(this.getClass().getName());
    }

    public void handle(GetCommand getCommand) {
        String key = null;
        try {
            key = URLDecoder.decode(getCommand.getKey(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new HazelcastException(e);
        }
        String mapName = DEFAULT_MAP_NAME;
        int index = key.indexOf(':');
        if (index != -1) {
            mapName = MAP_NAME_PRECEDER + key.substring(0, index);
            key = key.substring(index + 1);
        }
        Object value = textCommandService.get(mapName, key);
        MemcacheEntry entry = null;
        if (value != null) {
            if (value instanceof MemcacheEntry) {
                entry = (MemcacheEntry) value;
            } else if (value instanceof byte[]) {
                entry = new MemcacheEntry(getCommand.getKey(), ((byte[]) value), 0);
            } else if (value instanceof String) {
                entry = new MemcacheEntry(getCommand.getKey(), stringToBytes((String) value), 0);
            } else {
                try {
                    entry = new MemcacheEntry(getCommand.getKey(), textCommandService.toByteArray(value), 0);
                } catch (Exception e) {
                    logger.warning(e);
                }
            }
        }
        if (entry != null) {
            textCommandService.incrementGetHitCount();
        } else {
            textCommandService.incrementGetMissCount();
        }
        getCommand.setValue(entry, single);
        textCommandService.sendResponse(getCommand);
    }

    public void handleRejection(GetCommand getCommand) {
        getCommand.setValue(null, single);
        textCommandService.sendResponse(getCommand);
    }
}
