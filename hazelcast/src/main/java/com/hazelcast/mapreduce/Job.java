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

/**
 * <p>
 * This interface describes a mapreduce Job that is build by {@link JobTracker#newJob(KeyValueSource)}.<br>
 * It is used to execute mappings and calculations on the different cluster nodes and reduce or collate these mapped
 * values to results.
 * </p>
 * <p>
 * Implementations returned by the JobTracker are fully threadsafe and can be used concurrently and multiple
 * times <b>once the configuration is finished</b>.
 * </p>
 * <p>
 * <b>Caution: The generic types of Jobs change depending on the used methods which can make it necessary to use
 * different assignment variables when used over multiple source lines.</b>
 * </p>
 * <p>
 * An example on how to use it:
 * <pre>
 * HazelcastInstance hz = getHazelcastInstance();
 * IMap&lt;Integer, Integer> map = (...) hz.getMap( "default" );
 * JobTracker tracker = hz.getJobTracker( "default" );
 * Job&lt;Integer, Integer> job = tracker.newJob( KeyValueSource.fromMap( map ) );
 * ICompletableFuture&lt;Map&lt;String, Integer>> future = job
 *      .mapper( buildMapper() ).reducer( buildReducer() ).submit();
 * Map&lt;String, Integer> results = future.get();
 * </pre>
 * </p>
 *
 * @param <KeyIn>   type of key used as input key type
 * @param <ValueIn> type of value used as input value type
 * @since 3.2
 */
@Beta
public interface Job<KeyIn, ValueIn> {

    /**
     * Defines keys to execute the mapper and a possibly defined reducer against. If keys are known before submitting
     * the task setting them can improve execution speed.
     *
     * @param keys keys to be executed against
     * @return instance of this Job with generics changed on usage
     */
    Job<KeyIn, ValueIn> onKeys(Iterable<KeyIn> keys);

    /**
     * Defines keys to execute the mapper and a possibly defined reducer against. If keys are known before submitting
     * the task setting them can improve execution speed.
     *
     * @param keys keys to be executed against
     * @return instance of this Job with generics changed on usage
     */
    Job<KeyIn, ValueIn> onKeys(KeyIn... keys);

    /**
     * Defines the number of elements per chunk. Whenever the chunk size is reached and a
     * {@link com.hazelcast.mapreduce.ReducerFactory} is defined the chunk will be send to the nodes that
     * is responsible for the emitted keys.<br/>
     * <b>Please note, that chunks are deactivated when no ReducerFactory is defined</b>
     *
     * @param chunkSize the number of elements per chunk
     * @return instance of this Job with generics changed on usage
     */
    Job<KeyIn, ValueIn> chunkSize(int chunkSize);

    /**
     * Defines the strategy to handle topology changes while executing the map reduce job. For further
     * information see {@link com.hazelcast.mapreduce.TopologyChangedStrategy}.
     *
     * @param topologyChangedStrategy strategy to use
     * @return instance of this Job with generics changed on usage
     */
    Job<KeyIn, ValueIn> topologyChangedStrategy(TopologyChangedStrategy topologyChangedStrategy);

    /**
     * Defines the {@link KeyPredicate} implementation to preselect keys the MapReduce task will be executed on.
     * Preselecting keys can speed up the job massively.<br>
     * This method can be used in conjunction with {@link #onKeys(Iterable)} or {@link #onKeys(Object...)} to define a
     * range of known and evaluated keys.
     *
     * @param predicate predicate implementation to be used to evaluate keys
     * @return instance of this Job with generics changed on usage
     */
    Job<KeyIn, ValueIn> keyPredicate(KeyPredicate<KeyIn> predicate);

    /**
     * Defines the mapper for this task. This method is not idempotent and can be callable only one time. Further
     * calls result in an {@link IllegalStateException} to be thrown telling you to not change the internal state.
     *
     * @param mapper     tasks mapper
     * @param <KeyOut>   type of the emitted key
     * @param <ValueOut> type of the emitted value
     * @return instance of this Job with generics changed on usage
     */
    <KeyOut, ValueOut> MappingJob<KeyIn, KeyOut, ValueOut> mapper(Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper);

}
