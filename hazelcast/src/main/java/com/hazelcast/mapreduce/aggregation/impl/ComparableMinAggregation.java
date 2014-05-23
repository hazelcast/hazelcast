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
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;

import java.util.Map;

public class ComparableMinAggregation<Key, Value>
        implements AggType<Key, Value, Key, Comparable, Comparable, Comparable, Comparable> {

    @Override
    public Collator<Map.Entry<Key, Comparable>, Comparable> getCollator() {
        return new Collator<Map.Entry<Key, Comparable>, Comparable>() {
            @Override
            public Comparable collate(Iterable<Map.Entry<Key, Comparable>> values) {
                Comparable min = null;
                for (Map.Entry<Key, Comparable> entry : values) {
                    Comparable value = entry.getValue();
                    if (min == null || value.compareTo(min) < 0) {
                        min = value;
                    }
                }
                return min;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Comparable> getMapper(Supplier<Key, Value, Comparable> supplier) {
        return new SupplierConsumingMapper<Key, Value, Comparable>(supplier);
    }

    @Override
    public CombinerFactory<Key, Comparable, Comparable> getCombinerFactory() {
        return new ComparableMaxCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, Comparable, Comparable> getReducerFactory() {
        return new ComparableMaxReducerFactory<Key>();
    }

    static final class ComparableMaxCombinerFactory<Key>
            implements CombinerFactory<Key, Comparable, Comparable> {

        @Override
        public Combiner<Key, Comparable, Comparable> newCombiner(Key key) {
            return new ComparableMaxCombiner<Key>();
        }
    }

    static final class ComparableMaxReducerFactory<Key>
            implements ReducerFactory<Key, Comparable, Comparable> {

        @Override
        public Reducer<Key, Comparable, Comparable> newReducer(Key key) {
            return new ComparableMaxReducer<Key>();
        }
    }

    private static final class ComparableMaxCombiner<Key>
            extends Combiner<Key, Comparable, Comparable> {

        private Comparable chunkMin = null;

        @Override
        public void combine(Key key, Comparable value) {
            if (chunkMin == null || value.compareTo(chunkMin) < 0) {
                chunkMin = value;
            }
        }

        @Override
        public Comparable finalizeChunk() {
            Comparable value = chunkMin;
            chunkMin = null;
            return value;
        }
    }

    private static final class ComparableMaxReducer<Key>
            extends Reducer<Key, Comparable, Comparable> {

        private volatile Comparable min = null;

        @Override
        public void reduce(Comparable value) {
            if (min == null || value.compareTo(min) < 0) {
                min = value;
            }
        }

        @Override
        public Comparable finalizeReduce() {
            return min;
        }
    }
}
