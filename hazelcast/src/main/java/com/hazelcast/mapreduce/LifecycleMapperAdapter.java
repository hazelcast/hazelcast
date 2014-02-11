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
 * The abstract LifecycleMapperAdapter superclass is used to ease building mappers for the {@link Job}.
 * Most mappers will only implement the {@link #map(Object, Object, Context)} method to collect and
 * emit needed key-value pairs.<br>
 * For more complex algorithms there is the possibility to override the {@link #initialize(Context)} and
 * {@link #finalized(Context)} methods as well.
 * </p>
 * <p>
 * A simple mapper could look like the following example:
 * <p/>
 * <pre>
 * public static class MyMapper extends LifecycleMapperAdapter&lt;Integer, Integer, String, Integer>
 * {
 *   public void map( Integer key, Integer value, Context&lt;String, Integer> collector )
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
 * @param <KeyIn>    type of key used in the {@link KeyValueSource}
 * @param <ValueIn>  type of value used in the {@link KeyValueSource}
 * @param <KeyOut>   key type for mapped results
 * @param <ValueOut> value type for mapped results
 * @since 3.2
 */
@Beta
public abstract class LifecycleMapperAdapter<KeyIn, ValueIn, KeyOut, ValueOut>
        implements LifecycleMapper<KeyIn, ValueIn, KeyOut, ValueOut> {

    /**
     * {@inheritDoc}
     */
    public void initialize(Context<KeyOut, ValueOut> context) {
    }

    /**
     * {@inheritDoc}
     */
    public abstract void map(KeyIn key, ValueIn value, Context<KeyOut, ValueOut> context);

    /**
     * {@inheritDoc}
     */
    public void finalized(Context<KeyOut, ValueOut> context) {
    }

}
