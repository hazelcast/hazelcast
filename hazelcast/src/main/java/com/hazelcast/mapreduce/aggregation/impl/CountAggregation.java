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
import com.hazelcast.mapreduce.aggregation.Supplier;

import java.util.Map;

public class CountAggregation<Key, Value>
        implements AggType<Key, Value, Key, Object, Long, Long, Long> {

    @Override
    public Collator<Map.Entry<Key, Long>, Long> getCollator() {
        return new Collator<Map.Entry<Key, Long>, Long>() {
            @Override
            public Long collate(Iterable<Map.Entry<Key, Long>> values) {
                long count = 0;
                for (Map.Entry<Key, Long> entry : values) {
                    count += entry.getValue();
                }
                return count;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Object> getMapper(Supplier<Key, Value, Object> supplier) {
        return new SupplierConsumingMapper<Key, Value, Object>(supplier);
    }

    @Override
    public CombinerFactory<Key, Object, Long> getCombinerFactory() {
        return new CountCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, Long, Long> getReducerFactory() {
        return new CountReducerFactory<Key>();
    }

    static final class CountCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, Object, Long> {

        @Override
        public Combiner<Key, Object, Long> newCombiner(Key key) {
            return new CountCombiner<Key>();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.COUNT_COMBINER_FACTORY;
        }
    }

    static final class CountReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, Long, Long> {

        @Override
        public Reducer<Key, Long, Long> newReducer(Key key) {
            return new CountReducer<Key>();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.COUNT_REDUCER_FACTORY;
        }
    }

    private static final class CountCombiner<Key>
            extends Combiner<Key, Object, Long> {

        private long chunkCount;

        @Override
        public void combine(Key key, Object value) {
            chunkCount++;
        }

        @Override
        public Long finalizeChunk() {
            long value = chunkCount;
            chunkCount = 0;
            return value;
        }
    }

    private static final class CountReducer<Key>
            extends Reducer<Key, Long, Long> {

        private volatile long count;

        @Override
        public void reduce(Long value) {
            count += value;
        }

        @Override
        public Long finalizeReduce() {
            return count;
        }
    }
}
