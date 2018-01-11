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

import com.hazelcast.core.DistributedObject;

/**
 * <p>
 * The JobTracker interface is used to create instances of {@link Job}s depending
 * on the given data structure / data source.
 * </p>
 * <p>
 * The underlying created instance of the {@link Job} depends on whether it is used for a
 * {@link com.hazelcast.client.HazelcastClient} or a {@link com.hazelcast.core.Hazelcast} member node.
 * </p>
 * <p>
 * The default usage is same for both cases and looks similar to the following example:<br>
 * <p/>
 * <pre>
 * HazelcastInstance hz = getHazelcastInstance();
 * IMap map = hz.getMap( getMapName() );
 * JobTracker tracker = hz.getJobTracker( "default" );
 * Job job = tracker.newJob( KeyValueSource.fromMap( map ) );
 * </pre>
 * </p>
 * <p>
 * The created instance of JobTracker is fully threadsafe and can be used concurrently and multiple times.<br>
 * <b>Caution: Do not use the JobTracker with data structures of other {@link com.hazelcast.core.HazelcastInstance}
 * instances than the one used for creation of the JobTracker. Unexpected results may happen!</b>
 * </p>
 *
 * @since 3.2
 * @deprecated MapReduce is deprecated and will be removed in 4.0.
 * For map aggregations, you can use {@link com.hazelcast.aggregation.Aggregator} on IMap.
 * For general data processing, it is superseded by <a href="http://jet.hazelcast.org">Hazelcast Jet</a>.
 */
@Deprecated
public interface JobTracker
        extends DistributedObject {

    /**
     * Builds a {@link Job} instance for the given {@link KeyValueSource} instance. The returning
     * implementation depends on the {@link com.hazelcast.core.HazelcastInstance} that created
     * the JobTracker.<br>
     * <b>Caution: Do not use the JobTracker with data structures of other
     * {@link com.hazelcast.core.HazelcastInstance} instances than the one used for creation of the
     * JobTracker. Unexpected results may happen!</b>
     *
     * @param source data source the created Job should work on
     * @param <K> type of the key
     * @param <V> type of the value
     * @return instance of the Job bound to the given KeyValueSource
     */
    <K, V> Job<K, V> newJob(KeyValueSource<K, V> source);

    /**
     * Builds a complex {@link ProcessJob} instance for the given {@link KeyValueSource} instance. The returning
     * implementation is depending on the {@link com.hazelcast.core.HazelcastInstance} that was creating the
     * JobTracker.<br>
     * <b>Caution: Do not use the JobTracker with data structures of other
     * {@link com.hazelcast.core.HazelcastInstance} instances than the one used for creation of the JobTracker.
     * Unexpected results may happen!</b>
     *
     * @param source data source that the created Job should work on
     * @return instance of the ProcessJob bound to the given KeyValueSource
     */
    // This feature is moved to Hazelcast 3.3
    //<K, V> ProcessJob<K, V> newProcessJob(KeyValueSource<K, V> source);

    /**
     * Returns an implementation of {@link TrackableJob}, or null if the job ID is not available
     * or the job is already finished.
     *
     * @param jobId job ID to search the TrackableJob for
     * @param <V>   type of the resulting value
     * @return a trackable job for given job ID or null if the job ID is not available
     */
    <V> TrackableJob<V> getTrackableJob(String jobId);
}
