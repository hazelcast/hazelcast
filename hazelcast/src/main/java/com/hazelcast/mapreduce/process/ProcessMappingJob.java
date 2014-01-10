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

package com.hazelcast.mapreduce.process;

import com.hazelcast.core.CompletableFuture;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.Map;

public interface ProcessMappingJob<KeyIn, ValueIn>
        extends ProcessJob<KeyIn, ValueIn> {

    /**
     * Defines the {@link CombinerFactory} for this task. This method is not idempotent and is callable only one time. Further
     * calls result in an {@link IllegalStateException} to be thrown telling you to not change the internal state.
     *
     * @param combinerFactory CombinerFactory to build Combiner
     * @return instance of this ProcessJob with generics changed on usage
     */
    <ValueOut> ProcessMappingJob<KeyIn, ValueOut> combiner(CombinerFactory<KeyIn, ValueIn, ValueOut> combinerFactory);

    /**
     * Defines the {@link ReducerFactory} for this task. This method is not idempotent and is callable only one time. Further
     * calls result in an {@link IllegalStateException} to be thrown telling you to not change the internal state.
     *
     * @param reducerFactory ReducerFactory to build Reducers
     * @return instance of this ProcessJob with generics changed on usage
     */
    <ValueOut> ProcessReducingJob<KeyIn, ValueOut> reducer(ReducerFactory<KeyIn, ValueIn, ValueOut> reducerFactory);

    /**
     * Submits the task to Hazelcast and executes the defined mapper and reducer on all cluster nodes
     *
     * @return CompletableFuture to wait for mapped and possibly reduced result
     */
    CompletableFuture<Map<KeyIn, ValueIn>> submit();

    /**
     * Submits the task to Hazelcast and executes the defined mapper and reducer on all cluster nodes and executes the
     * collator before returning the final result.
     *
     * @param collator collator to use after map and reduce
     * @return CompletableFuture to wait for mapped and possibly reduced result
     */
    <ValueOut> CompletableFuture<ValueOut> submit(Collator<Map<KeyIn, ValueIn>, ValueOut> collator);

}
