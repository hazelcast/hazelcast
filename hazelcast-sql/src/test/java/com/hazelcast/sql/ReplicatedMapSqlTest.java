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

package com.hazelcast.sql;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.sql.impl.QueryPlan;
import com.hazelcast.sql.impl.SqlCursorImpl;
import com.hazelcast.sql.support.ModelGenerator;
import com.hazelcast.sql.support.model.person.City;
import com.hazelcast.sql.support.SqlTestSupport;
import com.hazelcast.sql.support.plan.PhysicalPlanChecker;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
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
    private HazelcastInstance member;
    private HazelcastInstance liteMember;

    @Before
    public void before() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        member = nodeFactory.newHazelcastInstance();
        liteMember = nodeFactory.newHazelcastInstance(new Config().setLiteMember(true));

        ModelGenerator.generatePerson(member);
    }

    @Test
    public void testReplicatedProject() {
        ReplicatedMap<Long, City> cityMap = member.getReplicatedMap(ModelGenerator.CITY);

        int expSize = cityMap.size();

        Set<String> expNames = new HashSet<>();

        for (City city : cityMap.values()) {
            expNames.add(city.getName());
        }

        try (SqlCursorImpl cursor = executeQuery(member, "SELECT name FROM city")) {
            QueryPlan plan = cursor.getHandle().getPlan();

            assertEquals(1, plan.getFragments().size());

            assertPlan(
                plan.getFragments().get(0),
                PhysicalPlanChecker.newBuilder()
                    .addReplicatedMapScan(ModelGenerator.CITY, expressions(keyValueExtractorExpression("name")), null)
                    .addRoot()
                    .build()
            );

            List<SqlRow> rows = getQueryRows(cursor);

            assertEquals(expSize, rows.size());

            for (SqlRow row : rows) {
                String name = row.getColumn(0);

                assertTrue(expNames.contains(name));
            }
        }
    }

    @Test
    public void testReplicatedFilter() {
        ReplicatedMap<Long, City> cityMap = member.getReplicatedMap(ModelGenerator.CITY);

        Map.Entry<Long, City> city = cityMap.entrySet().iterator().next();

        String name = city.getValue().getName();

        try (SqlCursorImpl cursor = executeQuery(member, "SELECT name FROM city WHERE name='" + name + "'")) {
            QueryPlan plan = cursor.getHandle().getPlan();

            assertEquals(1, plan.getFragments().size());

            List<SqlRow> rows = getQueryRows(cursor);

            assertEquals(1, rows.size());
            assertEquals(name, rows.get(0).getColumn(0));
        }
    }

    @Test
    @Ignore
    // TODO: Support replicated map descriptors on lite member?
    public void testReplicatedProjectLite() {
        try (SqlCursorImpl cursor = executeQuery(liteMember, "SELECT name FROM city")) {
            QueryPlan plan = cursor.getHandle().getPlan();

            assertEquals(2, plan.getFragments().size());

            List<SqlRow> rows = getQueryRows(cursor);

            assertEquals(ModelGenerator.CITY_CNT, rows.size());
        }
    }
}
