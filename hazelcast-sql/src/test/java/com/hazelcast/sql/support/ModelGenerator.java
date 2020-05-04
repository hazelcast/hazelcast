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

package com.hazelcast.sql.support;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.strategy.DeclarativePartitioningStrategy;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.sql.support.model.person.City;
import com.hazelcast.sql.support.model.person.Department;
import com.hazelcast.sql.support.model.person.Person;
import com.hazelcast.sql.support.model.person.PersonKey;

/**
 * Utility class which generate models for test cases.
 */
public class ModelGenerator {
    public static final int CITY_CNT = 2;
    public static final int DEPARTMENT_CNT = 2;
    public static final int PERSON_CNT = 10;

    public static final String CITY = "city";
    public static final String DEPARTMENT = "department";
    public static final String PERSON = "person";

    private ModelGenerator() {
        // No-op.
    }

    public static void generatePerson(HazelcastInstance member) {
        generatePerson(member, PERSON_CNT);
    }

    public static void generatePerson(HazelcastInstance member, int personCnt) {
        // Prepare config.
        ReplicatedMapConfig cityCfg = new ReplicatedMapConfig(CITY);
        cityCfg.setAsyncFillup(false);

        MapConfig departmentCfg = new MapConfig(DEPARTMENT);

        departmentCfg.setPartitioningStrategyConfig(
            new PartitioningStrategyConfig().setPartitioningStrategy(
                new DeclarativePartitioningStrategy<>()
            )
        );

        MapConfig personCfg = new MapConfig(PERSON);

        personCfg.setPartitioningStrategyConfig(
            new PartitioningStrategyConfig().setPartitioningStrategy(
                new DeclarativePartitioningStrategy<>().setField("deptId")
            )
        );

        member.getConfig().addReplicatedMapConfig(cityCfg);
        member.getConfig().addMapConfig(departmentCfg);
        member.getConfig().addMapConfig(personCfg);

        // Populate data.
        ReplicatedMap<Long, City> cityMap = member.getReplicatedMap(CITY);
        IMap<Long, Department> departmentMap = member.getMap(DEPARTMENT);
        IMap<PersonKey, Person> personMap = member.getMap(PERSON);

        for (int i = 0; i < CITY_CNT; i++) {
            cityMap.put((long) i, new City("city-" + i));
        }

        for (int i = 0; i < DEPARTMENT_CNT; i++) {
            departmentMap.put((long) i, new Department("department-" + i));
        }

        int age = 40;
        long salary = 1000;

        for (int i = 0; i < personCnt; i++) {
            int deptId = i % DEPARTMENT_CNT;

            PersonKey key = new PersonKey(i, deptId);

            Person val = new Person(
                "person-" + i,
                age++ % 80,
                salary * (i + 1),
                i % CITY_CNT,
                "department-" + deptId
            );

            personMap.put(key, val);
        }
    }
}
