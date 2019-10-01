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
import com.hazelcast.sql.model.ModelGenerator;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Basic test for replicated map.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapTest extends SqlTestSupport {
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
        List<SqlRow> rows = getQueryRows(
            member,
            "SELECT name FROM city"
        );

        assertEquals(ModelGenerator.CITY_CNT, rows.size());
    }
}
