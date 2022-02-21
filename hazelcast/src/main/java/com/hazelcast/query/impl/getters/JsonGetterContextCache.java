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

package com.hazelcast.query.impl.getters;

import com.hazelcast.internal.util.SampleableConcurrentHashMap;
import com.hazelcast.internal.util.SampleableConcurrentHashMap.SamplingEntry;

public class JsonGetterContextCache {

    private final int cleanupRemoveAtLeastItems;
    private final SampleableConcurrentHashMap<String, JsonGetterContext> internalCache;
    private final int maxContexts;

    public JsonGetterContextCache(int maxContexts, int cleanupRemoveAtLeastItems) {
        this.maxContexts = maxContexts;
        this.cleanupRemoveAtLeastItems = cleanupRemoveAtLeastItems;
        this.internalCache = new SampleableConcurrentHashMap<String, JsonGetterContext>(maxContexts);
    }

    /**
     * Returns an existing or newly created context for given query path.
     * If maximum cache size is reached, then some entries are evicted.
     * The newly created entry is not evicted.
     *
     * @param queryPath
     * @return
     */
    public JsonGetterContext getContext(String queryPath) {
        JsonGetterContext context = internalCache.get(queryPath);
        if (context != null) {
            return context;
        }
        context = new JsonGetterContext(queryPath);
        JsonGetterContext previousContextValue = internalCache.putIfAbsent(queryPath, context);
        if (previousContextValue == null) {
            cleanupIfNeccessary(context);
            return context;
        } else {
            return previousContextValue;
        }
    }

    /**
     * Cleanup on best effort basis. Concurrent calls to this method may
     * leave the cache empty. In that case, lost entries are re-cached
     * at a later call to {@link #getContext(String)}.
     *
     * @param excluded
     */
    private void cleanupIfNeccessary(JsonGetterContext excluded) {
        int cacheCount;
        while ((cacheCount = internalCache.size()) > maxContexts) {
            int sampleCount = Math.max(cacheCount - maxContexts, cleanupRemoveAtLeastItems) + 1;
            for (SamplingEntry sample: internalCache.getRandomSamples(sampleCount)) {
                if (excluded != sample.getEntryValue()) {
                    internalCache.remove(sample.getEntryKey());
                }
            }
        }
    }

    // for testing
    int getCacheSize() {
        return internalCache.size();
    }
}
