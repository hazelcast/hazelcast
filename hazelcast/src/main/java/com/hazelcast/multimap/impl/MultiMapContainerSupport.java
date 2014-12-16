/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.multimap.impl.ValueCollectionFactory.createCollection;

/**
 * Contains various {@link com.hazelcast.multimap.impl.MultiMapContainer} support methods.
 */
abstract class MultiMapContainerSupport {

    protected final String name;

    protected final NodeEngine nodeEngine;

    protected final MultiMapConfig config;

    protected final ConcurrentMap<Data, MultiMapWrapper> multiMapWrappers
            = new ConcurrentHashMap<Data, MultiMapWrapper>(1000);

    protected MultiMapContainerSupport(String name, NodeEngine nodeEngine) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.config = nodeEngine.getConfig().findMultiMapConfig(name);
    }


    public MultiMapWrapper getOrCreateMultiMapWrapper(Data dataKey) {
        MultiMapWrapper wrapper = multiMapWrappers.get(dataKey);
        if (wrapper != null) {
            return wrapper;
        }
        // create wrapper.
        final MultiMapConfig.ValueCollectionType valueCollectionType = config.getValueCollectionType();
        final Collection<MultiMapRecord> collection = createCollection(valueCollectionType);
        wrapper = new MultiMapWrapper(collection);

        multiMapWrappers.put(dataKey, wrapper);

        return wrapper;
    }

    public MultiMapWrapper getMultiMapWrapperOrNull(Data dataKey) {
        return multiMapWrappers.get(dataKey);
    }

    public ConcurrentMap<Data, MultiMapWrapper> getMultiMapWrappers() {
        return multiMapWrappers;
    }

    public MultiMapConfig getConfig() {
        return config;
    }


}
