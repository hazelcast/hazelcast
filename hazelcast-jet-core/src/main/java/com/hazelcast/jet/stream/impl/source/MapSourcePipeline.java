/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl.source;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.stream.impl.AbstractSourcePipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.Processors;

import java.util.Map;


public class MapSourcePipeline<K, V> extends AbstractSourcePipeline<Map.Entry<K, V>> {

    private final IMap<K, V> map;

    public MapSourcePipeline(StreamContext context, IMap<K, V> map) {
        super(context);
        this.map = map;
    }

    @Override
    protected ProcessorMetaSupplier getProducer() {
        return Processors.mapReader(map.getName());
    }

    @Override
    protected String getName() {
        return "map-reader-" + map.getName();
    }


}

