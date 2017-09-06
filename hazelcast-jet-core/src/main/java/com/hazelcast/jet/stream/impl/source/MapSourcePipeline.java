/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.processor.SourceProcessors;
import com.hazelcast.jet.stream.impl.pipeline.AbstractSourcePipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;

import java.util.Map;


public class MapSourcePipeline<K, V, T> extends AbstractSourcePipeline<T> {

    private final IMap<K, V> map;
    private final DistributedFunction<Map.Entry<K, V>, T> projectionF;
    private final DistributedPredicate<Map.Entry<K, V>> predicate;


    public MapSourcePipeline(StreamContext context, IMap<K, V> map,
                             DistributedPredicate<Map.Entry<K, V>> predicate,
                             DistributedFunction<Map.Entry<K, V>, T> projectionF) {
        super(context);
        this.map = map;
        this.predicate = predicate;
        this.projectionF = projectionF;
    }

    @Override
    protected ProcessorMetaSupplier getSourceMetaSupplier() {
        if (projectionF != null) {
            return SourceProcessors.readMap(map.getName(), predicate, projectionF);
        }
        return SourceProcessors.readMap(map.getName());
    }

    @Override
    protected String getName() {
        return "read-map-" + map.getName();
    }


}

