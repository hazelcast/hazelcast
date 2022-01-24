/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map.index;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionType;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.BIG_DECIMAL;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.BIG_INTEGER;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.BOOLEAN;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.BYTE;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.CHARACTER;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.DOUBLE;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.FLOAT;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.INTEGER;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.LONG;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.SHORT;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.STRING;

abstract class SqlIndexTestSupport extends OptimizerTestSupport {
    @SuppressWarnings("rawtypes")
    protected static String toLiteral(ExpressionType type, Object value) {
        if (type == BOOLEAN) {
            return Boolean.toString((Boolean) value);
        } else if (type == BYTE) {
            return Byte.toString((Byte) value);
        } else if (type == SHORT) {
            return Short.toString((Short) value);
        } else if (type == INTEGER) {
            return Integer.toString((Integer) value);
        } else if (type == LONG) {
            return Long.toString((Long) value);
        } else if (type == BIG_DECIMAL) {
            return value.toString();
        } else if (type == BIG_INTEGER) {
            return value.toString();
        } else if (type == FLOAT) {
            return Float.toString((Float) value);
        } else if (type == DOUBLE) {
            return Double.toString((Double) value);
        } else if (type == STRING) {
            return "'" + value + "'";
        } else if (type == CHARACTER) {
            return "'" + value + "'";
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    protected static List<ExpressionType<?>> baseTypes() {
        return Arrays.asList(
                BOOLEAN,
                INTEGER,
                STRING
        );
    }

    protected static List<ExpressionType<?>> nonBaseTypes() {
        return Arrays.asList(
                BYTE,
                SHORT,
                LONG,
                BIG_DECIMAL,
                BIG_INTEGER,
                FLOAT,
                DOUBLE,
                CHARACTER
        );
    }

    protected static List<ExpressionType<?>> allTypes() {
        return Arrays.asList(
                BOOLEAN,
                BYTE,
                SHORT,
                INTEGER,
                LONG,
                BIG_DECIMAL,
                BIG_INTEGER,
                FLOAT,
                DOUBLE,
                STRING,
                CHARACTER
        );
    }

    protected static <K> K getLocalKey(
            HazelcastInstance member,
            IntFunction<K> keyProducer
    ) {
        return getLocalKeys(member, 1, keyProducer).get(0);
    }

    protected static <K> List<K> getLocalKeys(
            HazelcastInstance member,
            int count,
            IntFunction<K> keyProducer
    ) {
        return new ArrayList<>(getLocalEntries(member, count, keyProducer, keyProducer).keySet());
    }

    protected static <K, V> Map<K, V> getLocalEntries(
            HazelcastInstance member,
            int count,
            IntFunction<K> keyProducer,
            IntFunction<V> valueProducer
    ) {
        if (count == 0) {
            return Collections.emptyMap();
        }

        PartitionService partitionService = member.getPartitionService();

        Map<K, V> res = new LinkedHashMap<>();

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            K key = keyProducer.apply(i);

            if (key == null) {
                continue;
            }

            Partition partition = partitionService.getPartition(key);

            if (!partition.getOwner().localMember()) {
                continue;
            }

            V value = valueProducer.apply(i);

            if (value == null) {
                continue;
            }

            res.put(key, value);

            if (res.size() == count) {
                break;
            }
        }

        if (res.size() < count) {
            throw new RuntimeException("Failed to get the necessary number of keys: " + res.size());
        }

        return res;
    }
}
