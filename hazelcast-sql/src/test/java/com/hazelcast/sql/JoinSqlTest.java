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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.impl.QueryFragment;
import com.hazelcast.sql.impl.QueryPlan;
import com.hazelcast.sql.impl.SqlCursorImpl;
import com.hazelcast.sql.support.ModelGenerator;
import com.hazelcast.sql.support.SqlTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static junit.framework.TestCase.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JoinSqlTest extends SqlTestSupport {
    protected HazelcastInstance member;

    @Before
    public void before() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        member = nodeFactory.newHazelcastInstance();
        nodeFactory.newHazelcastInstance();

        ModelGenerator.generatePerson(member);
    }

    @Test
    public void testEquiPartitionedPartitionedNonCollocated() {
        SqlCursorImpl cursor = executeQuery(
            member,
            "SELECT p.deptTitle, d.title FROM person p INNER JOIN department d ON p.deptTitle = d.title"
//            "SELECT p.name, p.deptTitle FROM person p INNER JOIN department d ON p.deptTitle = d.title"
        );

        List<SqlRow> rows = getQueryRows(cursor);

        assertEquals(1, rows.size());
    }

    @Test
    public void testJoinConditionOn() {
        List<SqlRow> res = getQueryRows(
            member,
            "SELECT p.name, d.title FROM person p INNER JOIN department d ON p.deptId = d.__key"
        );

        Assert.assertEquals(ModelGenerator.PERSON_CNT, res.size());
    }

    @Test
    public void testJoinConditionWhere() {
        QueryPlan planWhere = getPlan(
            member,
            "SELECT p.name, d.title FROM person p, department d WHERE p.deptId = d.__key"
        );

        QueryPlan planOn = getPlan(
            member,
            "SELECT p.name, d.title FROM person p INNER JOIN department d ON p.deptId = d.__key"
        );

        QueryFragment fragmentOn = planOn.getFragments().get(0);
        QueryFragment fragmentWhere = planWhere.getFragments().get(0);

        Assert.assertEquals(fragmentOn.getNode(), fragmentWhere.getNode());
    }
}
