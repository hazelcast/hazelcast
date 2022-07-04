/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aggregation;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

final class TestSamples {

    private static final int NUMBER_OF_SAMPLE_VALUES = 10000;
    private static final String LOREM_IPSUM = "Lorem ipsum dolor sit amet consectetur adipiscing elit";

    private TestSamples() {
    }

    static <T> Map.Entry<T, T> createEntryWithValue(T value) {
        return new MapEntrySimple<T, T>(value, value);
    }

    static <T> Map.Entry<T, T> createExtractableEntryWithValue(T value, InternalSerializationService ss) {
        return new ExtractableEntry<T, T>(value, value, ss);
    }

    static List<Integer> sampleIntegers() {
        return sampleValues(new RandomNumberSupplier<Integer>() {
            @Override
            protected Integer mapFrom(Number value) {
                return value.intValue();
            }
        });
    }

    static List<Long> sampleLongs() {
        return sampleValues(new RandomNumberSupplier<Long>() {
            @Override
            protected Long mapFrom(Number value) {
                return value.longValue();
            }
        });
    }

    static Collection<Float> sampleFloats() {
        return sampleValues(new RandomNumberSupplier<Float>() {
            @Override
            protected Float mapFrom(Number value) {
                return value.floatValue();
            }
        });
    }

    static List<Double> sampleDoubles() {
        return sampleValues(new RandomNumberSupplier<Double>() {
            @Override
            protected Double mapFrom(Number value) {
                return value.doubleValue();
            }
        });
    }

    static List<BigDecimal> sampleBigDecimals() {
        return sampleValues(new RandomNumberSupplier<BigDecimal>() {
            @Override
            protected BigDecimal mapFrom(Number value) {
                return BigDecimal.valueOf(value.doubleValue());
            }
        });
    }

    static List<BigInteger> sampleBigIntegers() {
        return sampleValues(new RandomNumberSupplier<BigInteger>() {
            @Override
            protected BigInteger mapFrom(Number value) {
                return BigInteger.valueOf(value.longValue());
            }
        });
    }

    static List<String> sampleStrings() {
        return new ArrayList(asList(LOREM_IPSUM.split(" ")));
    }

    static List<Person> samplePersons() {
        List<Person> personList = new ArrayList<Person>(NUMBER_OF_SAMPLE_VALUES);
        for (Double age : sampleDoubles()) {
            personList.add(new Person(age));
        }
        return personList;
    }

    static List<ValueContainer> sampleValueContainers(ValueContainer.ValueType valueType) {
        List<ValueContainer> containerList = new ArrayList<ValueContainer>(NUMBER_OF_SAMPLE_VALUES);
        switch (valueType) {
            case INTEGER:
                for (int intValue : sampleIntegers()) {
                    containerList.add(new ValueContainer(intValue));
                }
                break;
            case LONG:
                for (long longValue : sampleLongs()) {
                    containerList.add(new ValueContainer(longValue));
                }
                break;
            case FLOAT:
                for (float floatValue : sampleFloats()) {
                    containerList.add(new ValueContainer(floatValue));
                }
                break;
            case DOUBLE:
                for (double doubleValue : sampleDoubles()) {
                    containerList.add(new ValueContainer(doubleValue));
                }
                break;
            case BIG_DECIMAL:
                for (BigDecimal bigDecimal : sampleBigDecimals()) {
                    containerList.add(new ValueContainer(bigDecimal));
                }
                break;
            case BIG_INTEGER:
                for (BigInteger bigInteger : sampleBigIntegers()) {
                    containerList.add(new ValueContainer(bigInteger));
                }
                break;
            case NUMBER:
                new ArrayList<ValueContainer>();
                for (Long longValue : sampleLongs()) {
                    createNumberContainer(containerList, longValue);
                }
                for (Integer intValue : sampleIntegers()) {
                    createNumberContainer(containerList, intValue);
                }
                break;
            case STRING:
                for (String string : sampleStrings()) {
                    containerList.add(new ValueContainer(string));
                }
                break;
        }
        return containerList;
    }

    static void addValues(List<ValueContainer> containerList, ValueContainer.ValueType valueType) {
        switch (valueType) {
            case DOUBLE:
                for (Double doubleValue : sampleDoubles()) {
                    createNumberContainer(containerList, doubleValue);
                }
                break;
            case BIG_INTEGER:
                for (BigInteger bigInteger : sampleBigIntegers()) {
                    createNumberContainer(containerList, bigInteger);
                }
                break;
        }
    }

    private static <T extends Number> List<T> sampleValues(RandomNumberSupplier<T> randomNumberSupplier) {
        List<T> numbers = new ArrayList<T>();
        for (int i = 0; i < NUMBER_OF_SAMPLE_VALUES; i++) {
            numbers.add(randomNumberSupplier.get());
        }
        return numbers;
    }

    private static void createNumberContainer(List<ValueContainer> values, Number value) {
        ValueContainer container = new ValueContainer();
        container.numberValue = value;
        values.add(container);
    }

    private static final class ExtractableEntry<K, V> extends QueryableEntry<K, V> {

        private K key;
        private V value;

        ExtractableEntry(K key, V value, InternalSerializationService ss) {
            this.extractors = Extractors.newBuilder(ss).build();
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public Data getKeyData() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Data getValueData() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Object getTargetObject(boolean key) {
            return key ? this.key : this.value;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V newValue) {
            V oldValue = this.value;
            this.value = newValue;
            return oldValue;
        }

        void setKey(K key) {
            this.key = key;
        }

        public void setSerializationService(InternalSerializationService serializationService) {
            this.serializationService = serializationService;
        }

        @Override
        public K getKeyIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public Data getKeyDataIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public V getValueIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public Data getValueDataIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ExtractableEntry<?, ?> that = (ExtractableEntry<?, ?>) o;

            if (key != null ? !key.equals(that.key) : that.key != null) {
                return false;
            }
            return value != null ? value.equals(that.value) : that.value == null;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ExtractableEntry{key=" + key + ", value=" + value + '}';
        }
    }
}
