/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.mapreduce.impl.task.DefaultContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * The predefined distinct value aggregation.
 *
 * @param <Key>          the input key type
 * @param <Value>        the input value type
 * @param <DistinctType> the common super type for all distinct values
 */
public class DistinctValuesAggregation<Key, Value, DistinctType>
        implements AggType<Key, Value, Integer, DistinctType, Set<DistinctType>, Set<DistinctType>, Set<DistinctType>> {

    private static final int DEFAULT_DISTRIBUTION_FACTOR = 20;

    @Override
    public Collator<Map.Entry<Integer, Set<DistinctType>>, Set<DistinctType>> getCollator() {
        return new Collator<Map.Entry<Integer, Set<DistinctType>>, Set<DistinctType>>() {

            @Override
            public Set<DistinctType> collate(Iterable<Map.Entry<Integer, Set<DistinctType>>> values) {
                Set<DistinctType> distinctValues = new HashSet<DistinctType>();
                for (Map.Entry<Integer, Set<DistinctType>> value : values) {
                    distinctValues.addAll(value.getValue());
                }
                return distinctValues;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Integer, DistinctType> getMapper(Supplier<Key, Value, DistinctType> supplier) {
        return new DistinctValueMapper<Key, Value, DistinctType>(supplier);
    }

    @Override
    public CombinerFactory<Integer, DistinctType, Set<DistinctType>> getCombinerFactory() {
        return new DistinctValuesCombinerFactory<DistinctType>();
    }

    @Override
    public ReducerFactory<Integer, Set<DistinctType>, Set<DistinctType>> getReducerFactory() {
        return new DistinctValuesReducerFactory<DistinctType>();
    }

    /**
     * Distinct values CombinerFactory
     *
     * @param <DistinctType> the distinct values type
     */
    @BinaryInterface
    static class DistinctValuesCombinerFactory<DistinctType>
            extends AbstractAggregationCombinerFactory<Integer, DistinctType, Set<DistinctType>> {

        @Override
        public Combiner<DistinctType, Set<DistinctType>> newCombiner(Integer key) {
            return new DistinctValuesCombiner<DistinctType>();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.DISTINCT_VALUES_COMBINER_FACTORY;
        }
    }

    /**
     * Distinct values Combiner
     *
     * @param <DistinctType> the distinct values type
     */
    @BinaryInterface
    private static class DistinctValuesCombiner<DistinctType>
            extends Combiner<DistinctType, Set<DistinctType>> {

        private final Set<DistinctType> distinctValues = new HashSet<DistinctType>();

        @Override
        public void combine(DistinctType value) {
            distinctValues.add(value);
        }

        @Override
        public Set<DistinctType> finalizeChunk() {
            Set<DistinctType> distinctValues = new SetAdapter<DistinctType>();
            distinctValues.addAll(this.distinctValues);
            this.distinctValues.clear();
            return distinctValues;
        }
    }

    /**
     * Distinct values ReducerFactory
     *
     * @param <DistinctType> the distinct values type
     */
    @BinaryInterface
    static class DistinctValuesReducerFactory<DistinctType>
            extends AbstractAggregationReducerFactory<Integer, Set<DistinctType>, Set<DistinctType>> {

        @Override
        public Reducer<Set<DistinctType>, Set<DistinctType>> newReducer(Integer key) {
            return new DistinctValuesReducer<DistinctType>();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.DISTINCT_VALUES_REDUCER_FACTORY;
        }
    }

    /**
     * Distinct values Reducer
     *
     * @param <DistinctType> the distinct values type
     */
    private static class DistinctValuesReducer<DistinctType>
            extends Reducer<Set<DistinctType>, Set<DistinctType>> {

        private final Set<DistinctType> distinctValues = new SetAdapter<DistinctType>();

        @Override
        public void reduce(Set<DistinctType> value) {
            distinctValues.addAll(value);
        }

        @Override
        public Set<DistinctType> finalizeReduce() {
            return distinctValues;
        }
    }

    /**
     * A special mapper for distributing reducing of distinct values
     *
     * @param <Key>          the input key type
     * @param <Value>        the input value type
     * @param <DistinctType> the type of distinct values
     */
    @SuppressFBWarnings("SE_NO_SERIALVERSIONID")
    @BinaryInterface
    static class DistinctValueMapper<Key, Value, DistinctType>
            implements Mapper<Key, Value, Integer, DistinctType>, IdentifiedDataSerializable {

        // These keys are used to distribute reducer steps around the cluster
        private static final int[] DISTRIBUTION_KEYS;

        static {
            Random random = new Random();
            DISTRIBUTION_KEYS = new int[DEFAULT_DISTRIBUTION_FACTOR];
            for (int i = 0; i < DISTRIBUTION_KEYS.length; i++) {
                DISTRIBUTION_KEYS[i] = random.nextInt();
            }
        }

        private transient SimpleEntry<Key, Value> entry = new SimpleEntry<Key, Value>();
        private transient int keyPosition;

        private Supplier<Key, Value, DistinctType> supplier;

        DistinctValueMapper() {
        }

        DistinctValueMapper(Supplier<Key, Value, DistinctType> supplier) {
            this.supplier = supplier;
        }

        @Override
        public void map(Key key, Value value, Context<Integer, DistinctType> context) {
            int mappingKey = key();
            entry.setKey(key);
            entry.setValue(value);
            entry.setSerializationService(((DefaultContext) context).getSerializationService());
            entry.setExtractors(Extractors.newBuilder(((DefaultContext) context).getSerializationService()).build());
            DistinctType valueOut = supplier.apply(entry);
            if (valueOut != null) {
                context.emit(mappingKey, valueOut);
            }
        }

        @Override
        public int getFactoryId() {
            return AggregationsDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.DISTINCT_VALUES_MAPPER;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {

            out.writeObject(supplier);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {

            supplier = in.readObject();
        }

        private int key() {
            if (keyPosition >= DISTRIBUTION_KEYS.length) {
                keyPosition = 0;
            }
            return keyPosition++;
        }
    }

}
