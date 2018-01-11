/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce;

import com.hazelcast.nio.serialization.BinaryInterface;

import java.io.Serializable;

/**
 * <p>
 * A CombinerFactory implementation is used to build {@link Combiner} instances per key.<br/>
 * An implementation needs to be serializable by Hazelcast since it is distributed together with
 * the {@link Mapper} implementation to run alongside.
 * </p>
 *
 * @param <KeyIn>    key type of the resulting keys
 * @param <ValueIn>  value type of the incoming values
 * @param <ValueOut> value type of the reduced values
 * @since 3.2
 * @deprecated MapReduce is deprecated and will be removed in 4.0.
 * For map aggregations, you can use {@link com.hazelcast.aggregation.Aggregator} on IMap.
 * For general data processing, it is superseded by <a href="http://jet.hazelcast.org">Hazelcast Jet</a>.
 */
@Deprecated
@BinaryInterface
public interface CombinerFactory<KeyIn, ValueIn, ValueOut>
        extends Serializable {

    /**
     * Build a new {@link Combiner} instance specific to the supplied key.
     *
     * @param key key that the new Combiner is build for
     * @return a Combiner instance specific for the given key
     */
    Combiner<ValueIn, ValueOut> newCombiner(KeyIn key);

}
