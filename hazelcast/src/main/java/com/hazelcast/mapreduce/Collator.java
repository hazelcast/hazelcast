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

/**
 * This interface can be implemented to define a Collator which is executed after calculation
 * of the MapReduce algorithm on remote cluster nodes but before returning the final result.<br>
 * Collator can, for example, be used to sum up a final value.
 *
 * @param <ValueIn>  value type of the resulting values
 * @param <ValueOut> type for the collated result
 * @since 3.2
 * @deprecated MapReduce is deprecated and will be removed in 4.0.
 * For map aggregations, you can use {@link com.hazelcast.aggregation.Aggregator} on IMap.
 * For general data processing, it is superseded by <a href="http://jet.hazelcast.org">Hazelcast Jet</a>.
 */
@Deprecated
public interface Collator<ValueIn, ValueOut> {

    /**
     * This method is called with the mapped and possibly reduced values from the MapReduce algorithm.
     *
     * @param values The mapped and possibly reduced values from the MapReduce algorithm
     * @return The collated result
     */
    ValueOut collate(Iterable<ValueIn> values);

}
