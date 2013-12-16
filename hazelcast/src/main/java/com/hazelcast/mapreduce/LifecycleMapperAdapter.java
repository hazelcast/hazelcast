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

/**
 * <p>
 * The abstract LifecycleMapperAdapter superclass is used to build mappers for the {@link com.hazelcast.mapreduce.Job}.
 * Most mappers will only implement the {@link #map(Object, Object, com.hazelcast.mapreduce.Context)} method to collect and
 * emit needed key-value pairs.<br>
 * For more complex algorithms there is the possibility to override the {@link #initialize(com.hazelcast.mapreduce.Context)} and
 * {@link #finalized(com.hazelcast.mapreduce.Context)} methods as well.
 * </p>
 * <p>
 * A simple mapper could look like the following example:
 * <p/>
 * <pre>
 * public static class MyMapper extends LifecycleMapperAdapter<Integer, Integer, String, Integer>
 * {
 *   public void map( Integer key, Integer value, Collector<String, Integer> collector )
 *   {
 *     collector.emit( String.valueOf( key ), value );
 *   }
 * }
 * </pre>
 * </p>
 * <p>
 * If you want to know more about the implementation of MapReduce algorithms read the {@see <a
 * href="http://research.google.com/archive/mapreduce-osdi04.pdf">Google Whitepaper on MapReduce</a>}.
 * </p>
 *
 * @param <KeyIn>    The type of key used in the {@link com.hazelcast.mapreduce.KeyValueSource}
 * @param <ValueIn>  The type of value used in the {@link com.hazelcast.mapreduce.KeyValueSource}
 * @param <KeyOut>   The key type for mapped results
 * @param <ValueOut> The value type for mapped results
 * @author noctarius
 */
public abstract class LifecycleMapperAdapter<KeyIn, ValueIn, KeyOut, ValueOut> implements LifecycleMapper<KeyIn, ValueIn, KeyOut, ValueOut> {

    /**
     * This method is called before the {@link #map(Object, Object, com.hazelcast.mapreduce.Context)} method is executed for every value and
     * can be used to initialize the internal state of the mapper or to emit a special value.
     *
     * @param context The {@link com.hazelcast.mapreduce.Context} to be used for emitting values.
     */
    public void initialize(Context<KeyOut, ValueOut> context) {
    }

    /**
     * The map method is called for every single key-value pair in the bound {@link com.hazelcast.core.IMap} instance on this cluster node
     * and partition.<br>
     * Due to it's nature of a DataGrid Hazelcast distributes values all over the cluster and so this method is executed
     * on multiple servers at the same time.<br>
     * If you want to know more about the implementation of MapReduce algorithms read the {@see <a
     * href="http://research.google.com/archive/mapreduce-osdi04.pdf">Google Whitepaper on MapReduce</a>}.
     *
     * @param key     The key to map
     * @param value   The value to map
     * @param context The {@link com.hazelcast.mapreduce.Context} to be used for emitting values.
     */
    public abstract void map(KeyIn key, ValueIn value, Context<KeyOut, ValueOut> context);

    /**
     * This method is called after the {@link #map(Object, Object, com.hazelcast.mapreduce.Context)} method is executed for every value and
     * can be used to finalize the internal state of the mapper or to emit a special value.
     *
     * @param context The {@link com.hazelcast.mapreduce.Context} to be used for emitting values.
     */
    public void finalized(Context<KeyOut, ValueOut> context) {
    }

}
