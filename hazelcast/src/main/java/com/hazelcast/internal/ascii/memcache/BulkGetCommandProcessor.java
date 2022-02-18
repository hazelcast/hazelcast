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
import com.hazelcast.internal.util.collection.ComposedKeyMap;
import com.hazelcast.internal.util.collection.InternalSetMultimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.ascii.memcache.MemcacheUtils.parseMemcacheKey;

public class BulkGetCommandProcessor extends MemcacheCommandProcessor<BulkGetCommand> {

    private final EntryConverter entryConverter;

    public BulkGetCommandProcessor(TextCommandService textCommandService, EntryConverter entryConverter) {
        super(textCommandService);
        this.entryConverter = entryConverter;
    }

    @Override
    public void handle(BulkGetCommand request) {
        List<String> memcacheKeys = request.getKeys();

        InternalSetMultimap<String, String> keysPerMap = new InternalSetMultimap<String, String>();
        ComposedKeyMap<String, String, String> mapNameAndKey2memcacheKey = new ComposedKeyMap<String, String, String>();
        for (String memcacheKey : memcacheKeys) {
            MapNameAndKeyPair mapNameAndKeyPair = parseMemcacheKey(memcacheKey);
            String mapName = mapNameAndKeyPair.getMapName();
            String hzKey = mapNameAndKeyPair.getKey();
            keysPerMap.put(mapName, hzKey);
            mapNameAndKey2memcacheKey.put(mapName, hzKey, memcacheKey);
        }

        Collection<MemcacheEntry> allResults = new ArrayList<MemcacheEntry>();
        for (Map.Entry<String, Set<String>> mapKeys : keysPerMap.entrySet()) {
            String mapName = mapKeys.getKey();
            Set<String> keys = mapKeys.getValue();
            Collection<MemcacheEntry> mapResult = getAll(mapName, keys, mapNameAndKey2memcacheKey);
            allResults.addAll(mapResult);
        }
        int missCount = memcacheKeys.size() - allResults.size();
        for (int i = 0; i < missCount; i++) {
            textCommandService.incrementGetMissCount();
        }

        request.setResult(allResults);
        textCommandService.sendResponse(request);
    }

    private Collection<MemcacheEntry> getAll(String mapName, Set<String> keys,
                                             ComposedKeyMap<String, String, String> mapNameAndKey2memcacheKey) {
        Map<String, Object> entries = textCommandService.getAll(mapName, keys);
        Collection<MemcacheEntry> result = new ArrayList<MemcacheEntry>(entries.size());
        for (Map.Entry<String, Object> entry : entries.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            String origKey = mapNameAndKey2memcacheKey.get(mapName, key);
            MemcacheEntry memcacheEntry = entryConverter.toEntry(origKey, value);
            textCommandService.incrementGetHitCount();
            result.add(memcacheEntry);
        }
        return result;
    }

    @Override
    public void handleRejection(BulkGetCommand request) {
        throw new UnsupportedOperationException("not used, remove this method from the interface");
    }
}
