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
import com.hazelcast.sql.model.ModelGenerator;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlTest extends HazelcastTestSupport {
    private HazelcastInstance member;

    @Before
    public void before() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        member = nodeFactory.newHazelcastInstance();
        nodeFactory.newHazelcastInstance();

        ModelGenerator.generatePerson(member);
    }

    @Test(timeout = Long.MAX_VALUE)
    public void testReplicatedProject() {
        doQuery(
            member,
            "SELECT name FROM city"
        );
    }

    @Test(timeout = Long.MAX_VALUE)
    public void testJoin() {
        List<SqlRow> res = doQuery(
            member,
            "SELECT p.name, d.title FROM person p INNER JOIN department d ON p.deptId = d.__key"
        );

        Assert.assertEquals(ModelGenerator.PERSON_CNT, res.size());
    }

    private List<SqlRow> doQuery(HazelcastInstance target, String sql) {
        SqlCursor cursor = target.getSqlService().query(sql);

        List<SqlRow> rows = new ArrayList<>();

        for (SqlRow row : cursor) {
            rows.add(row);
        }

        print(rows);

        return rows;
    }

    private void print(List<SqlRow> rows) {
        System.out.println(">>> RESULT:");

        for (SqlRow row : rows) {
            System.out.println(">>>\t" + row);
        }
    }
}
