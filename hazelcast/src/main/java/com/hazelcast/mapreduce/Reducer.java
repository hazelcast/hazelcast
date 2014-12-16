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
 * The abstract Reducer class is used to build reducers for the {@link Job}.<br>
 * Reducers may be distributed inside of the cluster but there is always only one Reducer
 * per key.
 * <p/>
 * Reducers are always called in a thread-safe way however they may be moved from one thread to
 * another in the internal thread pool. As of version 3.3.3 the internal state is automatically
 * ensured to be visible according to the Java memory model by the framework. The previous
 * requirement for making fields volatile is dropped.
 * </p>
 * <p>
 * Due to the fact that there is only one Reducer per key mapped values needs to be
 * transmitted to one of the cluster nodes. To reduce the traffic costs between the
 * nodes a {@link Combiner} implementation can be added to the call which runs alongside
 * the mapper to pre-reduce mapped values into intermediate results.
 * </p>
 * <p>
 * A simple Reducer implementation could look like that sum-function implementation:
 * <pre>
 * public class SumReducer implements Reducer&lt;Integer, Integer>
 * {
 *   private int sum = 0;
 *
 *   public void reduce( Integer value )
 *   {
 *     sum += value;
 *   }
 *
 *   public Integer finalizeReduce()
 *   {
 *     return sum;
 *   }
 * }
 * </pre>
 * </p>
 *
 * @param <ValueIn>  value type of the incoming values
 * @param <ValueOut> value type of the reduced values
 * @since 3.2
 */
@Beta
public abstract class Reducer<ValueIn, ValueOut> {

    /**
     * This method is called before the first value is submitted to this Reducer instance.
     * It can be used to setup any internal needed state before starting to reduce the
     * actual values.
     */
    public void beginReduce() {
    }

    /**
     * This method is called to supply values to be reduced into a final reduced result.<br/>
     * The reduce method might be called multiple times so the eventually reduces value
     * needs to be hold internally in a member state of the Reducer.
     *
     * @param value value to be reduced
     */
    public abstract void reduce(ValueIn value);

    /**
     * finalizeReduce is called as last step for a reducing phase per key and retrieved the
     * final reduced result.
     *
     * @return the final reduced result
     */
    public abstract ValueOut finalizeReduce();
}
