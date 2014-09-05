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

import java.io.Serializable;

/**
 * <p>
 * The interface Mapper is used to build mappers for the {@link Job}. Most mappers will only need to
 * implement this interface and the {@link #map(Object, Object, Context)} method to collect and emit needed
 * key-value pairs.<br>
 * For more complex algorithms there is the possibility to implement the {@link LifecycleMapper} interface and
 * override the {@link LifecycleMapper#initialize(Context)} and {@link LifecycleMapper#finalized(Context)}
 * methods as well.</p>
 * <p>
 * A simple mapper could look like the following example:
 * <pre>
 * public class MyMapper extends Mapper&lt;Integer, Integer, String, Integer>
 * {
 *   public void map( Integer key, Integer value, Context&lt;String, Integer> context )
 *   {
 *     context.emit( String.valueOf( key ), value );
 *   }
 * }
 * </pre>
 * </p>
 * <p>
 * If you want to know more about the implementation of MapReduce algorithms read the {@see <a
 * href="http://research.google.com/archive/mapreduce-osdi04.pdf">Google Whitepaper on MapReduce</a>}.
 * </p>
 *
 * @param <KeyIn>    The type of key used in the {@link KeyValueSource}
 * @param <ValueIn>  The type of value used in the {@link KeyValueSource}
 * @param <KeyOut>   The key type for mapped results
 * @param <ValueOut> The value type for mapped results
 * @since 3.2
 */
@Beta
public interface Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
        extends Serializable {

    /**
     * The map method is called for every single key-value pair in the bound {@link KeyValueSource} instance
     * on this cluster node and partition.<br>
     * Due to it's nature of a DataGrid Hazelcast distributes values all over the cluster and so this method
     * is executed on multiple servers at the same time.<br>
     * If you want to know more about the implementation of MapReduce algorithms read the {@see <a
     * href="http://research.google.com/archive/mapreduce-osdi04.pdf">Google Whitepaper on MapReduce</a>}.
     *
     * @param key     key to map
     * @param value   value to map
     * @param context Context to be used for emitting values
     */
    void map(KeyIn key, ValueIn value, Context<KeyOut, ValueOut> context);

}
