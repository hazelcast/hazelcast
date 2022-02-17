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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Date;

import static com.hazelcast.query.Predicates.equal;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DateHandlingPredicateTest extends HazelcastTestSupport {

    private static final long JUNE_2016_MILLIS = 1467110170001L;
    private static final Customer CUSTOMER_0 = new Customer(0);
    private static final Customer CUSTOMER_1 = new Customer(1);

    private IMap<Integer, Customer> map;

    @Before
    public void setup() {
        HazelcastInstance instance = createHazelcastInstance();
        map = instance.getMap("map");

        map.put(1, CUSTOMER_0);
        map.put(2, CUSTOMER_1);
    }

    @After
    public void tearDown() {
        shutdownNodeFactory();
    }

    @Test
    public void dateValueInPredicate() throws Exception {
        // date vs. date
        assertThat(
                map.values(equal("date", new java.util.Date(JUNE_2016_MILLIS))),
                allOf(hasItem(CUSTOMER_0), not(hasItem(CUSTOMER_1)))
        );

        // date vs. sqlDate
        assertThat(
                map.values(equal("date", new java.sql.Date(JUNE_2016_MILLIS))),
                allOf(hasItem(CUSTOMER_0), not(hasItem(CUSTOMER_1)))
        );

        // date vs. sqlTimestamp
        assertThat(
                map.values(equal("date", new java.sql.Timestamp(JUNE_2016_MILLIS))),
                allOf(hasItem(CUSTOMER_0), not(hasItem(CUSTOMER_1)))
        );

    }

    @Test
    public void sqlDateValueInPredicate() throws Exception {
        // sqlDate vs. date
        assertThat(
                map.values(equal("sqlDate", new java.util.Date(JUNE_2016_MILLIS))),
                allOf(hasItem(CUSTOMER_0), not(hasItem(CUSTOMER_1)))
        );

        // sqlDate vs. sqlDate
        assertThat(
                map.values(equal("sqlDate", new java.sql.Date(JUNE_2016_MILLIS))),
                allOf(hasItem(CUSTOMER_0), not(hasItem(CUSTOMER_1)))
        );

        // sqlDate vs. sqlTimestamp
        assertThat(
                map.values(equal("sqlDate", new java.sql.Timestamp(JUNE_2016_MILLIS))),
                allOf(hasItem(CUSTOMER_0), not(hasItem(CUSTOMER_1)))
        );

    }

    @Test
    public void sqlTimestampValueInPredicate() throws Exception {
        // sqlTimestamp vs. date
        assertThat(
                map.values(equal("sqlTimestamp", new java.util.Date(JUNE_2016_MILLIS))),
                allOf(hasItem(CUSTOMER_0), not(hasItem(CUSTOMER_1)))
        );

        // sqlTimestamp vs. sqlDate
        assertThat(
                map.values(equal("sqlTimestamp", new java.sql.Date(JUNE_2016_MILLIS))),
                allOf(hasItem(CUSTOMER_0), not(hasItem(CUSTOMER_1)))
        );

        // sqlTimestamp vs. sqlTimestamp
        assertThat(
                map.values(equal("sqlTimestamp", new java.sql.Timestamp(JUNE_2016_MILLIS))),
                allOf(hasItem(CUSTOMER_0), not(hasItem(CUSTOMER_1)))
        );

    }

    private static class Customer implements Serializable {

        private final int id;

        private final java.util.Date date;
        private final java.sql.Date sqlDate;
        private final java.sql.Timestamp sqlTimestamp;

        Customer(int id) {
            this.id = id;

            this.date = new Date(JUNE_2016_MILLIS + id);
            this.sqlDate = new java.sql.Date(JUNE_2016_MILLIS + id);
            this.sqlTimestamp = new java.sql.Timestamp(JUNE_2016_MILLIS + id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Customer customer = (Customer) o;
            return id == customer.id;

        }

        @Override
        public int hashCode() {
            return id;
        }
    }
}
