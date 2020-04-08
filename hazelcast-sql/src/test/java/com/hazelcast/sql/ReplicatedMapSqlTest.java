/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.SqlCursorImpl;
import com.hazelcast.sql.support.ModelGenerator;
import com.hazelcast.sql.support.SqlTestSupport;
import com.hazelcast.sql.support.model.person.City;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Basic test for replicated map.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapSqlTest extends SqlTestSupport {
    private static TestHazelcastInstanceFactory factory;
    private static HazelcastInstance member;

    @BeforeClass
    public static void beforeClass() {
        factory = new TestHazelcastInstanceFactory(2);

        member = factory.newHazelcastInstance();

        ModelGenerator.generatePerson(member);
    }

    @AfterClass
    public static void afterClass() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    @Test
    public void testProject() {
        ReplicatedMap<Long, City> cityMap = member.getReplicatedMap(ModelGenerator.CITY);

        int expSize = cityMap.size();

        Set<String> expNames = new HashSet<>();

        for (City city : cityMap.values()) {
            expNames.add(city.getName());
        }

        try (SqlCursorImpl cursor = executeQueryEx(member, "SELECT name FROM city")) {
            Plan plan = cursor.getPlan();

            assertEquals(1, plan.getFragmentCount());

            List<SqlRow> rows = getQueryRows(cursor);

            assertEquals(expSize, rows.size());

            for (SqlRow row : rows) {
                String name = row.getObject(0);

                assertTrue(expNames.contains(name));
            }
        }
    }

    @Test
    public void testProjectProject() {
        ReplicatedMap<Long, City> cityMap = member.getReplicatedMap(ModelGenerator.CITY);

        int expSize = cityMap.size();

        Set<String> expNames = new HashSet<>();

        for (City city : cityMap.values()) {
            expNames.add(city.getName());
        }

        try (SqlCursorImpl cursor = executeQueryEx(member, "SELECT name FROM (SELECT name FROM city)")) {
            Plan plan = cursor.getPlan();

            assertEquals(1, plan.getFragmentCount());

            List<SqlRow> rows = getQueryRows(cursor);

            assertEquals(expSize, rows.size());

            for (SqlRow row : rows) {
                String name = row.getObject(0);

                assertTrue(expNames.contains(name));
            }
        }
    }

    @Test
    public void testProjectFilter() {
        ReplicatedMap<Long, City> cityMap = member.getReplicatedMap(ModelGenerator.CITY);

        Map.Entry<Long, City> city = cityMap.entrySet().iterator().next();

        String name = city.getValue().getName();

        try (SqlCursorImpl cursor = executeQueryEx(member, "SELECT name FROM city WHERE name='" + name + "'")) {
            Plan plan = cursor.getPlan();

            assertEquals(1, plan.getFragmentCount());

            List<SqlRow> rows = getQueryRows(cursor);

            assertEquals(1, rows.size());
            assertEquals(name, rows.get(0).getObject(0));
        }
    }

    // TODO: Tests for interleaved project and filter (need filter pushdown rule).
    // TODO: Tests for aggregations
    // TODO: Tests for joins
}
