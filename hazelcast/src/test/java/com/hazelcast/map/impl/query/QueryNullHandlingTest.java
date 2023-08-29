/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.query.impl.predicates.BetweenPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.query.impl.predicates.GreaterLessPredicate;
import com.hazelcast.query.impl.predicates.InPredicate;
import com.hazelcast.query.impl.predicates.NotEqualPredicate;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.config.IndexType.BITMAP;
import static com.hazelcast.config.IndexType.HASH;
import static com.hazelcast.config.IndexType.SORTED;
import static com.hazelcast.query.Predicates.between;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.greaterEqual;
import static com.hazelcast.query.Predicates.greaterThan;
import static com.hazelcast.query.Predicates.in;
import static com.hazelcast.query.Predicates.instanceOf;
import static com.hazelcast.query.Predicates.lessEqual;
import static com.hazelcast.query.Predicates.lessThan;
import static com.hazelcast.query.Predicates.notEqual;
import static com.hazelcast.query.impl.IndexCopyBehavior.COPY_ON_READ;
import static com.hazelcast.query.impl.IndexCopyBehavior.COPY_ON_WRITE;
import static com.hazelcast.query.impl.IndexCopyBehavior.NEVER;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryNullHandlingTest extends HazelcastTestSupport {

    @Parameter(0)
    public IndexType indexType;

    @Parameter(1)
    public IndexCopyBehavior copyBehavior;

    @Parameters(name = "indexType: {0}, copyBehavior: {1}")
    public static Collection<Object[]> parameters() {
        //@formatter:off
        return asList(new Object[][]{
                {null, null},

                {SORTED, COPY_ON_READ},
                {SORTED, COPY_ON_WRITE},
                {SORTED, NEVER},

                {HASH, COPY_ON_READ},
                {HASH, COPY_ON_WRITE},
                {HASH, NEVER},

                {BITMAP, COPY_ON_READ},
                {BITMAP, COPY_ON_WRITE},
                {BITMAP, NEVER}
        });
        //@formatter:on
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2");
        if (indexType != null) {
            MapConfig mapConfig = config.getMapConfig("map");
            mapConfig.addIndexConfig(new IndexConfig(indexType, "value"));
        }
        if (copyBehavior != null) {
            config.setProperty(ClusterProperty.INDEX_COPY_BEHAVIOR.getName(), copyBehavior.name());
        }
        return config;
    }

    @Test
    public void test() {
        verify(createHazelcastInstance().getMap("map"), indexType);
    }

    public static void verify(IMap<Integer, Int> map, IndexType indexType) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<Integer> values = Arrays.asList(0, 1, 2, 2, 5, null, null);
        Collections.shuffle(values, random);
        for (Integer value : values) {
            map.put(random.nextInt(), new Int(value));
        }

        verify(map, indexType, null, 0, 1, 2, 2, 5, null, null);

        verify(map, indexType, lessThan("value", 10), 0, 1, 2, 2, 5);
        verify(map, indexType, greaterThan("value", 10));
        verify(map, indexType, lessThan("value", 2), 0, 1);
        verify(map, indexType, greaterThan("value", 2), 5);

        verify(map, indexType, lessEqual("value", 10), 0, 1, 2, 2, 5);
        verify(map, indexType, greaterEqual("value", 10));
        verify(map, indexType, lessEqual("value", 2), 0, 1, 2, 2);
        verify(map, indexType, greaterEqual("value", 2), 2, 2, 5);

        verify(map, indexType, equal("value", 2), 2, 2);
        verify(map, indexType, equal("value", null), null, null);

        verify(map, indexType, notEqual("value", 2), 0, 1, 5, null, null);
        verify(map, indexType, notEqual("value", null), 0, 1, 2, 2, 5);

        verify(map, indexType, in("value", (Comparable<?>) null), null, null);
        verify(map, indexType, in("value", 1, null), 1, null, null);

        verify(map, indexType, between("value", 0, 5), 0, 1, 2, 2, 5);
        verify(map, indexType, between("value", 1, 2), 1, 2, 2);

        verify(map, indexType, instanceOf(Int.class), 0, 1, 2, 2, 5, null, null);
    }

    private static void verify(IMap<Integer, Int> map, IndexType indexType, Predicate<Integer, Int> predicate,
                               Integer... expected) {
        long indexQueriesBefore = map.getLocalMapStats().getIndexedQueryCount();
        Collection<Int> actual = predicate == null ? map.values() : map.values(predicate);
        long indexQueriesAfter = map.getLocalMapStats().getIndexedQueryCount();

        boolean indexQueryExpected = isIndexQueryExpected(indexType, predicate);
        if (indexQueryExpected && indexQueriesBefore == indexQueriesAfter) {
            fail("Expected index query, got none");
        } else if (!indexQueryExpected && indexQueriesBefore != indexQueriesAfter) {
            fail("Unexpected index query");
        }

        assertEquals("Expected " + Arrays.toString(expected) + ", got " + actual, expected.length, actual.size());
        compare:
        for (Int i : actual) {
            Integer v = i.value;
            for (int j = 0; j < expected.length; ++j) {
                if (Objects.equals(expected[j], v)) {
                    expected[j] = -1;
                    continue compare;
                }
            }
            fail("Unexpected value: " + v + " in " + actual + ", expected " + Arrays.toString(expected));
        }
    }

    private static boolean isIndexQueryExpected(IndexType indexType, Predicate<Integer, Int> p) {
        if (indexType == null) {
            return false;
        } else if (indexType == SORTED) {
            return p instanceof EqualPredicate || p instanceof GreaterLessPredicate || p instanceof InPredicate
                    || p instanceof BetweenPredicate;
        } else if (indexType == HASH) {
            return p instanceof EqualPredicate || p instanceof GreaterLessPredicate || p instanceof InPredicate
                    || p instanceof BetweenPredicate;
        } else if (indexType == BITMAP) {
            return p instanceof EqualPredicate || p instanceof NotEqualPredicate || p instanceof InPredicate;
        } else {
            throw new RuntimeException("Unexpected index type");
        }
    }

    public static class Int implements Serializable {

        public Integer value;

        public Int(Integer value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "" + value;
        }

    }

}
