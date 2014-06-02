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

package com.hazelcast.mapreduce.aggregation;

import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.spi.annotation.Beta;

import java.util.Map;

/**
 * The Aggregation interface combines multiple map-reduce operations steps to a
 * single operation definition. It is a predefined set of {@link com.hazelcast.mapreduce.Mapper},
 * {@link com.hazelcast.mapreduce.Combiner}, {@link com.hazelcast.mapreduce.Reducer} and
 * probably a {@link com.hazelcast.mapreduce.Collator} to execute an aggregation over a
 * supplied set of data.
 * <p/>
 * As you'll see in the following example the Aggregations API is fully type-safe. Do to the lack of
 * full type inference support on Java 6 and Java 7 it seems more verbose than it actually is.
 * <pre>
 * IMap&lt;String, Employee> map = hazelcastInstance.getMap("employees");
 * Supplier&lt;String, Employee, Integer> supplier = Supplier.all((employee) -> employee.getSalaryPerMonth());
 * Aggregation&lt;String, Integer, Integer> aggregation = Aggregations.integerAvg();
 * int avgSalary = map.aggregate(supplier, aggregation);
 * </pre>
 * With Java 8 it is possible to write that all in just one line so that the API becomes very straight forward.
 * <pre>
 * IMap&lt;String, Employee> map = hazelcastInstance.getMap("employees");
 * int avgSalary = map.aggregate(Supplier.all((employee) -> employee.getSalaryPerMonth(), Aggregations.integerAvg());
 * </pre>
 *
 * @param <Key>      the input key type
 * @param <Supplied> the value type returned from the {@link com.hazelcast.mapreduce.aggregation.Supplier}
 * @param <Result>   the value type returned from the aggregation
 * @since 3.3
 */
@Beta
public interface Aggregation<Key, Supplied, Result> {

    /**
     * Returns a Collator implementation used in this aggregation
     *
     * @return the aggregation defined Collator
     */
    Collator<Map.Entry, Result> getCollator();

    /**
     * Returns the Mapper for this aggregation. The Mapper implementation has to handle
     * the {@link com.hazelcast.mapreduce.aggregation.Supplier} to filter / transform
     * values before emitting them to the further aggregation steps.
     *
     * @param supplier the Supplier to filter or / and transform values
     * @return the aggregation defined Mapper
     */
    Mapper getMapper(Supplier<Key, ?, Supplied> supplier);

    /**
     * Returns the CombinerFactory for this aggregation to pre-reduce values on mapping
     * nodes. Returning a CombinerFactory preserves traffic costs but implementing a
     * {@link com.hazelcast.mapreduce.Combiner} is not always possible.
     *
     * @return the aggregation defined CombinerFactory or null
     */
    CombinerFactory getCombinerFactory();

    /**
     * Returns the ReducerFactory for this aggregation. If a CombinerFactory is defined
     * the implemented {@link com.hazelcast.mapreduce.Reducer} has to handle values of
     * the returned type of the {@link com.hazelcast.mapreduce.Combiner}.
     *
     * @return the aggregation defined ReducerFactory
     */
    ReducerFactory getReducerFactory();
}
