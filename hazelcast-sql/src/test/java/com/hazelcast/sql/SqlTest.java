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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlTest extends HazelcastTestSupport {

//    private static final String QUERY = "SELECT __key FROM persons ORDER BY name";
//    private static final String QUERY = "select height + age from persons order by name";
    private static final String QUERY = "select POSITION('Per' in name), UPPER(name) || LOWER(name) FROM persons ORDER BY name";

    @Test
    public void testSimpleQuery() throws Exception {
        // Start several members.
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        Config cfg = new Config();

        HazelcastInstance member1 = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);

        // Add some data.
        for (int i = 0; i < 100; i++)
            member1.getMap("persons").put(i, new Person(i));

        // Execute.
        SqlCursor cursor = member1.getSqlService().query(QUERY);

        List<SqlRow> res = new ArrayList<>();

        for (SqlRow row : cursor)
            res.add(row);

        System.out.println(">>> SIZE: " + res.size());
        System.out.println(">>> RES:  " + res);
    }

    @SuppressWarnings("WeakerAccess")
    public static class Person implements Serializable {
        private static final long serialVersionUID = -221704179714350820L;

        public final int __key;
        public final String name;
        public final int age;
        public final double height;
        public final boolean active;

        public Person(int key) {
            this.__key = key;
            this.name = "Person " + key;
            this.age = ThreadLocalRandom.current().nextInt(100);
            this.height = ThreadLocalRandom.current().nextDouble(170);
            this.active = ThreadLocalRandom.current().nextBoolean();
        }

    }
}
