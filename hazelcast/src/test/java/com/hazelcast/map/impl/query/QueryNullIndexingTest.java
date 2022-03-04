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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class QueryNullIndexingTest extends HazelcastTestSupport {

    @Parameter(0)
    public IndexCopyBehavior copyBehavior;

    @Parameters(name = "copyBehavior: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {IndexCopyBehavior.COPY_ON_READ},
                {IndexCopyBehavior.COPY_ON_WRITE},
                {IndexCopyBehavior.NEVER},
        });
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithLessPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.lessThan("date", 5000000L));
        assertEquals(2, dates.size());
        assertContainsAll(dates, asList(2000000L, 4000000L));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithLessEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.lessEqual("date", 5000000L));
        assertEquals(2, dates.size());
        assertContainsAll(dates, asList(2000000L, 4000000L));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithGreaterPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.greaterThan("date", 5000000L));
        assertEquals(3, dates.size());
        assertContainsAll(dates, asList(6000000L, 8000000L, 10000000L));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithGreaterEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.greaterEqual("date", 6000000L));
        assertEquals(3, dates.size());
        assertContainsAll(dates, asList(6000000L, 8000000L, 10000000L));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithNotEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.notEqual("date", 2000000L));
        assertEquals(9, dates.size());
        assertContainsAll(dates, asList(4000000L, 6000000L, 8000000L, 10000000L, null));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithLessPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.lessThan("date", 5000000L));
        assertEquals(2, dates.size());
        assertContainsAll(dates, asList(2000000L, 4000000L));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithLessEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.lessEqual("date", 5000000L));
        assertEquals(2, dates.size());
        assertContainsAll(dates, asList(2000000L, 4000000L));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithGreaterPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.greaterThan("date", 5000000L));
        assertEquals(3, dates.size());
        assertContainsAll(dates, asList(6000000L, 8000000L, 10000000L));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithGreaterEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.greaterEqual("date", 6000000L));
        assertEquals(3, dates.size());
        assertContainsAll(dates, asList(6000000L, 8000000L, 10000000L));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithNotEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.notEqual("date", 2000000L));
        assertEquals(9, dates.size());
        assertContainsAll(dates, asList(4000000L, 6000000L, 8000000L, 10000000L, null));
    }

    private List<Long> queryIndexedDateFieldAsNullValue(boolean ordered,
                                                        Predicate<Integer, SampleTestObjects.Employee> pred) {
        Config config = getConfig();
        config.setProperty(ClusterProperty.INDEX_COPY_BEHAVIOR.getName(), copyBehavior.name());
        config.setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, SampleTestObjects.Employee> map = instance.getMap("default");

        map.addIndex(ordered ? IndexType.SORTED : IndexType.HASH, "date");
        for (int i = 10; i >= 1; i--) {
            Employee employee = new Employee(i, "name-" + i, i, true, i * 100);
            if (i % 2 == 0) {
                employee.setDate(new Timestamp(i * 1000000));
            } else {
                employee.setDate(null);
            }
            map.put(i, employee);
        }

        List<Long> dates = new ArrayList<Long>();
        for (SampleTestObjects.Employee employee : map.values(pred)) {
            Timestamp date = employee.getDate();
            dates.add(date == null ? null : date.getTime());
        }

        return dates;
    }
}
