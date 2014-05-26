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
 * The abstract Combiner class is used to build combiners for the {@link Job}.<br>
 * Those Combiners are distributed inside of the cluster and are running alongside
 * the {@link Mapper} implementations in the same node.<br/>
 * <b>Combiners are called in a threadsafe way so internal locking is not required.</b>
 * </p>
 * <p>
 * Combiners are normally used to build intermediate results on the mapping nodes to
 * lower the traffic overhead between the different nodes before the reducing phase.<br/>
 * Combiners need to be capable of combining data in multiple chunks to create a more
 * streaming like internal behavior.
 * </p>
 * <p>
 * A simple Combiner implementation in combination with a {@link Reducer} could look
 * like that avg-function implementation:
 * <pre>
 * public class AvgCombiner implements Combiner&lt;Integer, Tuple&lt;Long, Long>>
 * {
 *   private long count;
 *   private long amount;
 *   public void combine(Integer value)
 *   {
 *     count++;
 *     amount += value;
 *   }
 *
 *   public Tuple&lt;Long, Long> finalizeChunk()
 *   {
 *     Tuple&lt;Long, Long> tuple = new Tuple&lt;>( count, amount );
 *     count = 0;
 *     amount = 0;
 *     return tuple;
 *   }
 * }
 *
 * public class SumReducer implements Reducer&lt;Tuple&lt;Long, Long>, Integer>
 * {
 *   private long count;
 *   private long amount;
 *   public void reduce( Tuple&lt;Long, Long> value )
 *   {
 *     count += value.getFirst();
 *     amount += value.getSecond();
 *   }
 *
 *   public Integer finalizeReduce()
 *   {
 *     return amount / count;
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
public abstract class Combiner<ValueIn, ValueOut> {

    /**
     * This method is called before the first value is submitted to this Combiner instance.
     * It can be used to setup any internal needed state before starting to combining the
     * actual values.<br/>
     * The method is called only one time and is not called again before starting a new chunk.
     */
    public void beginCombine() {
    }

    /**
     * This method is called to supply values to be combined into an intermediate result chunk.<br/>
     * The combine method might be called multiple times so the combined chunk needs to be hold
     * internally in a member state of the Combiner.<br/>
     * After this method is called you need to reset the internal state to prepare combining of
     * the next chunk.
     *
     * @param value value to be reduced
     */
    public abstract void combine(ValueIn value);

    /**
     * Creates a chunk of {@link ValueOut} to be send to the {@link Reducer} for the according
     * key.
     *
     * @return chunk of intermediate data
     */
    public abstract ValueOut finalizeChunk();

    /**
     * This method is called always after a chunk of data is retrieved. It is used to reset
     * the internal state of the Combiner. It is equivalent to resetting the state inside of
     * {@link #finalizeChunk()} as with the last version of the API.
     */
    public void reset() {
    }

    /**
     * This method is called after mapping phase is over. It is intended to be overridden
     * to cleanup internal state and free possible resources.
     */
    public void finalizeCombine() {
    }

}
