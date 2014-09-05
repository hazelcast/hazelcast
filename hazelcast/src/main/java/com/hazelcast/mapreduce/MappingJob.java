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

package com.hazelcast.mapreduce;

import com.hazelcast.spi.annotation.Beta;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * This interface describes a mapping mapreduce Job.<br>
 * For further information {@link Job}.
 * </p>
 *
 * @param <EntryKey> type of the original input key
 * @param <KeyIn>    type of key used as input key type
 * @param <ValueIn>  type of value used as input value type
 * @see com.hazelcast.mapreduce.Job
 * @since 3.2
 */
@Beta
public interface MappingJob<EntryKey, KeyIn, ValueIn> {

    /**
     * Defines keys to execute the mapper and a possibly defined reducer against. If keys are known before submitting
     * the task setting them can improve execution speed.
     *
     * @param keys keys to be executed against
     * @return instance of this Job with generics changed on usage
     */
    MappingJob<EntryKey, KeyIn, ValueIn> onKeys(Iterable<EntryKey> keys);

    /**
     * Defines keys to execute the mapper and a possibly defined reducer against. If keys are known before submitting
     * the task setting them can improve execution speed.
     *
     * @param keys keys to be executed against
     * @return instance of this Job with generics changed on usage
     */
    MappingJob<EntryKey, KeyIn, ValueIn> onKeys(EntryKey... keys);

    /**
     * Defines the {@link KeyPredicate} implementation to preselect keys the MapReduce task will be executed on.
     * Preselecting keys can speed up the job massively.<br>
     * This method can be used in conjunction with {@link #onKeys(Iterable)} or {@link #onKeys(Object...)} to define a
     * range of known and evaluated keys.
     *
     * @param predicate predicate implementation to be used to evaluate keys
     * @return instance of this Job with generics changed on usage
     */
    MappingJob<EntryKey, KeyIn, ValueIn> keyPredicate(KeyPredicate<EntryKey> predicate);

    /**
     * Defines the number of elements per chunk. Whenever the chunk size is reached and a
     * {@link com.hazelcast.mapreduce.ReducerFactory} is defined the chunk will be send to the nodes that
     * is responsible for the emitted keys.<br/>
     * <b>Please note, that chunks are deactivated when no ReducerFactory is defined</b>
     *
     * @param chunkSize the number of elements per chunk
     * @return instance of this Job with generics changed on usage
     */
    MappingJob<EntryKey, KeyIn, ValueIn> chunkSize(int chunkSize);

    /**
     * Defines the strategy to handle topology changes while executing the map reduce job. For further
     * information see {@link com.hazelcast.mapreduce.TopologyChangedStrategy}.
     *
     * @param topologyChangedStrategy strategy to use
     * @return instance of this Job with generics changed on usage
     */
    MappingJob<EntryKey, KeyIn, ValueIn> topologyChangedStrategy(TopologyChangedStrategy topologyChangedStrategy);

    /**
     * Defines the {@link CombinerFactory} for this task. This method is not idempotent and is callable only one time. Further
     * calls result in an {@link IllegalStateException} to be thrown telling you to not change the internal state.
     *
     * @param combinerFactory CombinerFactory to build Combiner
     * @param <ValueOut>      type of the combined value
     * @return instance of this Job with generics changed on usage
     */
    <ValueOut> ReducingJob<EntryKey, KeyIn, ValueOut> combiner(CombinerFactory<KeyIn, ValueIn, ValueOut> combinerFactory);

    /**
     * Defines the {@link ReducerFactory} for this task. This method is not idempotent and is callable only one time. Further
     * calls result in an {@link IllegalStateException} to be thrown telling you to not change the internal state.
     *
     * @param reducerFactory ReducerFactory to build Reducers
     * @param <ValueOut>     type of the reduced value
     * @return instance of this Job with generics changed on usage
     */
    <ValueOut> ReducingSubmittableJob<EntryKey, KeyIn, ValueOut> reducer(ReducerFactory<KeyIn, ValueIn, ValueOut> reducerFactory);

    /**
     * Submits the task to Hazelcast and executes the defined mapper and reducer on all cluster nodes
     *
     * @return JobCompletableFuture to wait for mapped and possibly reduced result
     */
    JobCompletableFuture<Map<KeyIn, List<ValueIn>>> submit();

    /**
     * Submits the task to Hazelcast and executes the defined mapper and reducer on all cluster nodes and executes the
     * collator before returning the final result.
     *
     * @param collator   collator to use after map and reduce
     * @param <ValueOut> type of the collated value
     * @return JobCompletableFuture to wait for mapped and possibly reduced result
     */
    <ValueOut> JobCompletableFuture<ValueOut> submit(Collator<Map.Entry<KeyIn, List<ValueIn>>, ValueOut> collator);

}
