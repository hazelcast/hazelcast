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

import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.strategy.DeclarativePartitioningStrategy;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("serial")
public class SqlTest extends HazelcastTestSupport {
    private static final int CITY_CNT = 2;
    private static final int DEPARTMENT_CNT = 2;
    private static final int PERSON_CNT = 10;

    private HazelcastInstance member;

    @Before
    public void before() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        Config cfg = new Config();

        ReplicatedMapConfig cityCfg = new ReplicatedMapConfig("city");
        cityCfg.setAsyncFillup(false);

        MapConfig departmentCfg = new MapConfig("department");

        MapConfig personCfg = new MapConfig("person");

        personCfg.setPartitioningStrategyConfig(
            new PartitioningStrategyConfig().setPartitioningStrategy(
                new DeclarativePartitioningStrategy().setField("deptId")
            )
        );

        personCfg.addAttributeConfig(new AttributeConfig().setName("deptId").setPath("__key.deptId"));

        cfg.addReplicatedMapConfig(cityCfg);
        cfg.addMapConfig(departmentCfg);
        cfg.addMapConfig(personCfg);

        member = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<Long, City> cityMap = member.getReplicatedMap("city");
        IMap<Long, Department> departmentMap = member.getMap("department");
        IMap<PersonKey, Person> personMap = member.getMap("person");

        for (int i = 0; i < CITY_CNT; i++)
            cityMap.put((long)i, new City("city-" + i));

        for (int i = 0; i < DEPARTMENT_CNT; i++)
            departmentMap.put((long)i, new Department("department-" + i));

        int age = 40;
        long salary = 1000;

        for (int i = 0; i < PERSON_CNT; i++) {
            PersonKey key = new PersonKey(i, i % DEPARTMENT_CNT);

            Person val = new Person(
                "person-" + i,
                age++ % 80,
                salary * (i + 1),
                i % CITY_CNT
            );

            personMap.put(key, val);
        }

        System.out.println(">>> DATA LOAD COMPLETED");
    }

    @After
    public void after() {
        member = null;

        Hazelcast.shutdownAll();
    }

    @Test(timeout = Long.MAX_VALUE)
    public void testReplicatedProject() throws Exception {
        doQuery("SELECT name FROM city");
    }

    @Test(timeout = Long.MAX_VALUE)
    public void testJoin() throws Exception {
        List<SqlRow> res = doQuery("SELECT p.name, d.title FROM person p INNER JOIN department d ON p.deptId = d.__key");

        Assert.assertEquals(PERSON_CNT, res.size());
    }

    private List<SqlRow> doQuery(String sql) {
        SqlCursor cursor = member.getSqlService().query(sql);

        List<SqlRow> rows = new ArrayList<>();

        for (SqlRow row : cursor)
            rows.add(row);

        print(rows);

        return rows;
    }

    private void print(List<SqlRow> rows) {
        System.out.println(">>> RESULT:");

        for (SqlRow row : rows)
            System.out.println(">>>\t" + row);
    }

    private static class City implements Serializable {
        private String name;

        public City() {
            // No-op.
        }

        public City(String name) {
            this.name = name;
        }
    }

    private static class Department implements Serializable {
        private String title;

        public Department() {
            // No-op.
        }

        public Department(String title) {
            this.title = title;
        }
    }

    private static class PersonKey implements Serializable {
        private long id;
        private long deptId;

        public PersonKey() {
            // No-op.
        }

        public PersonKey(long id, long deptId) {
            this.id = id;
            this.deptId = deptId;
        }
    }

    private static class Person implements Serializable {
        private String name;
        private int age;
        private long salary;
        private long cityId;

        public Person() {
            // No-op.
        }

        public Person(String name, int age, long salary, long cityId) {
            this.name = name;
            this.age = age;
            this.salary = salary;
            this.cityId = cityId;
        }
    }
}
