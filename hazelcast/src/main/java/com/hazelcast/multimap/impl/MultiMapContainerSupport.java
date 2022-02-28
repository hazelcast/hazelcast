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

package com.hazelcast.multimap.impl;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.multimap.impl.ValueCollectionFactory.createCollection;
import static com.hazelcast.internal.util.MapUtil.createConcurrentHashMap;

/**
 * Contains various {@link com.hazelcast.multimap.impl.MultiMapContainer} support methods.
 */
abstract class MultiMapContainerSupport {

    protected final ConcurrentMap<Data, MultiMapValue> multiMapValues = createConcurrentHashMap(1000);

    protected final String name;
    protected final NodeEngine nodeEngine;
    protected final MultiMapConfig config;

    MultiMapContainerSupport(String name, NodeEngine nodeEngine) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.config = nodeEngine.getConfig().findMultiMapConfig(name);
    }

    public MultiMapValue getOrCreateMultiMapValue(Data dataKey) {
        MultiMapValue multiMapValue = multiMapValues.get(dataKey);
        if (multiMapValue != null) {
            return multiMapValue;
        }
        // create multiMapValue
        final MultiMapConfig.ValueCollectionType valueCollectionType = config.getValueCollectionType();
        final Collection<MultiMapRecord> collection = createCollection(valueCollectionType);
        multiMapValue = new MultiMapValue(collection);

        multiMapValues.put(dataKey, multiMapValue);

        return multiMapValue;
    }

    public MultiMapValue getMultiMapValueOrNull(Data dataKey) {
        return multiMapValues.get(dataKey);
    }

    public ConcurrentMap<Data, MultiMapValue> getMultiMapValues() {
        return multiMapValues;
    }

    public MultiMapConfig getConfig() {
        return config;
    }
}
