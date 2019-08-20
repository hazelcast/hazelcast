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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.strategy.DeclarativePartitioningStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlTest extends HazelcastTestSupport {

    private static final String QUERY = "SELECT __key, COUNT(DISTINCT LENGTH(name)), age, SUM(height) FROM city GROUP BY __key, age";
    // private static final String QUERY = "SELECT SUM(height) FROM city GROUP BY age ORDER BY SUM(height)";

    @Test(timeout = Long.MAX_VALUE)
    public void testSimpleQuery() throws Exception {
        // Start several members.
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        Config cfg = new Config();

//        cfg.addMapConfig(new MapConfig()
//            .setName("persons")
//            .setPartitioningStrategyConfig(
//                new PartitioningStrategyConfig()
//                    .setPartitioningStrategy(new DeclarativePartitioningStrategy().setField("key"))
//            )
//        );

        HazelcastInstance member1 = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);

        // Add some data.
        for (int i = 0; i < 10; i++)
            member1.getReplicatedMap("city").put(i, new City(i));

        for (int i = 0; i < 100; i++)
            member1.getMap("persons").put(new PersonKey(i), new Person(i));

        // Execute.
        SqlCursor cursor = member1.getSqlService().query(QUERY);

        List<SqlRow> res = new ArrayList<>();

        for (SqlRow row : cursor)
            res.add(row);

        System.out.println(">>> SIZE: " + res.size());
        System.out.println(">>> RES:  " + res);
    }

    public static class PersonKey implements Serializable {
        private static final long serialVersionUID = -2761952188092172459L;

        public final int key;
        public final String keyStr;

        public PersonKey(int key) {
            this.key = key;

            keyStr = Integer.toString(key);
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static class Person implements Serializable {
        private static final long serialVersionUID = -221704179714350820L;

        public final String name;
        public final int age;
        public final double height;
        public final boolean active;
        public final LocalDateTime birthDate = LocalDateTime.now();
        public final String birthDateString;
        public final Address address = new Address();
        public final List tokens = new ArrayList();

        public Person(int key) {

            this.name = "Person " + key;
            this.age = ThreadLocalRandom.current().nextInt(10);
            this.height = ThreadLocalRandom.current().nextDouble(170);
            this.active = ThreadLocalRandom.current().nextBoolean();

            birthDateString = LocalDateTime.now().toString();
        }
    }

    public static class Address implements Serializable {
        private static final long serialVersionUID = 8442512199470933111L;

        public final int apartment = ThreadLocalRandom.current().nextInt(100);
    }

    public static class City implements Serializable {
        private static final long serialVersionUID = -5759518310551454362L;

        public final String name;

        public City(int i) {
            this.name = "City-" + i;
        }
    }
}
