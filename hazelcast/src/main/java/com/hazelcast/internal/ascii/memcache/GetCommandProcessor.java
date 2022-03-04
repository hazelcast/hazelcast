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

package com.hazelcast.internal.ascii.memcache;

import com.hazelcast.internal.ascii.TextCommandService;

import static com.hazelcast.internal.ascii.memcache.MemcacheUtils.parseMemcacheKey;

public class GetCommandProcessor extends MemcacheCommandProcessor<GetCommand> {
    private final EntryConverter entryConverter;

    public GetCommandProcessor(TextCommandService textCommandService, EntryConverter entryConverter) {
        super(textCommandService);
        this.entryConverter = entryConverter;
    }

    @Override
    public void handle(GetCommand getCommand) {
        String memcacheKey = getCommand.getKey();
        MapNameAndKeyPair mapNameAndKeyPair = parseMemcacheKey(memcacheKey);
        Object value = textCommandService.get(mapNameAndKeyPair.getMapName(), mapNameAndKeyPair.getKey());
        MemcacheEntry entry = entryConverter.toEntry(memcacheKey, value);
        if (entry != null) {
            textCommandService.incrementGetHitCount();
        } else {
            textCommandService.incrementGetMissCount();
        }
        getCommand.setValue(entry);
        textCommandService.sendResponse(getCommand);
    }


    @Override
    public void handleRejection(GetCommand getCommand) {
        getCommand.setValue(null);
        textCommandService.sendResponse(getCommand);
    }
}
