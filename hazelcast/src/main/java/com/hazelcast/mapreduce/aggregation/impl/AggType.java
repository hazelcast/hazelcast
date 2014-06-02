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

package com.hazelcast.mapreduce.aggregation.impl;

import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.aggregation.Supplier;

import java.util.Map;

/**
 * The internal interface for Aggregation definitions to make those implementations more type-safe.
 * Internal AggTypes are adapted to {@link com.hazelcast.mapreduce.aggregation.Aggregation} using
 * an internal class inside {@link com.hazelcast.mapreduce.aggregation.Aggregations} which is not
 * part of the public API.
 *
 * @param <KeyIn>         the input key type
 * @param <ValueIn>       the input value type
 * @param <KeyOut>        the mappers output key type
 * @param <SuppliedValue> the supplied value type
 * @param <CombinerValue> the pre-reduced (combined) value type
 * @param <ReducerValue>  the reduced value type
 * @param <Result>        the final (mostly collated) value type
 */
public interface AggType<KeyIn, ValueIn, KeyOut, SuppliedValue, CombinerValue, ReducerValue, Result> {

    Collator<Map.Entry<KeyOut, ReducerValue>, Result> getCollator();

    Mapper<KeyIn, ValueIn, KeyOut, SuppliedValue> getMapper(Supplier<KeyIn, ValueIn, SuppliedValue> supplier);

    CombinerFactory<KeyOut, SuppliedValue, CombinerValue> getCombinerFactory();

    ReducerFactory<KeyOut, CombinerValue, ReducerValue> getReducerFactory();
}
