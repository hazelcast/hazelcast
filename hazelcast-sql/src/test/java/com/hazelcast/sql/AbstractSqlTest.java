package com.hazelcast.sql;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.strategy.DeclarativePartitioningStrategy;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * Base class for common SQL tests.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public abstract class AbstractSqlTest extends HazelcastTestSupport {
    private static final int CITY_CNT = 2;
    private static final int DEPARTMENT_CNT = 2;
    private static final int PERSON_CNT = 10;

    private static TestHazelcastFactory nodeFactory;

    protected static HazelcastInstance member;
    protected static HazelcastInstance liteMember;

    @Parameterized.Parameter
    public boolean executeFromLiteMember;

    @Parameterized.Parameters(name = "executeFromLiteMember:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
            { false },
            { true }
        });
    }

    @BeforeClass
    public static void beforeClass() {
        nodeFactory = new TestHazelcastFactory(3);

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

        nodeFactory.newHazelcastInstance(cfg);
        member = nodeFactory.newHazelcastInstance(cfg);
        //liteMember = nodeFactory.newHazelcastInstance(cfg.setLiteMember(true));

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

    @AfterClass
    public static void afterClass() {
        member = null;
        liteMember = null;

        nodeFactory.terminateAll();
    }

    protected List<SqlRow> doQuery(String sql) {
        HazelcastInstance target = executeFromLiteMember ? liteMember : member;

        SqlCursor cursor = target.getSqlService().query(sql);

        List<SqlRow> rows = new ArrayList<>();

        for (SqlRow row : cursor)
            rows.add(row);

        return rows;
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
