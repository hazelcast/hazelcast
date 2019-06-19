package com.hazelcast.sql;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.linq4j.Enumerable;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NewQueryTestFull extends HazelcastTestSupport {
    @Test
    public void testSqlSimple() throws Exception {
        // Start several members.
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        Config cfg = new Config();

        HazelcastInstance member1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance member2 = nodeFactory.newHazelcastInstance(cfg);

        String query = "select height from persons where age >= 5 order by name";
//        String query = "select age, height from persons where age >= 5";

        // Insert data.
        for (int i = 0; i < 100; i++)
            member1.getMap("queryMap").put(i, new Person(i));

        HazelcastSql hazelcastSql = HazelcastSql.createFor(member1);

        Enumerable<Object> res = hazelcastSql.query(query);

        for (Object object : res)
            System.out.println(object instanceof Object[] ? Arrays.deepToString((Object[]) object) : object);
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> asList(T... elems) {
        if (elems == null || elems.length == 0)
            return Collections.emptyList();

        ArrayList<T> res = new ArrayList<>(elems.length);

        Collections.addAll(res, elems);

        return res;
    }

    @SuppressWarnings("WeakerAccess")
    public static class Person implements Serializable {
        public final int __key;
        public final String name;
        public final int age;
        public final double height;
        public final boolean active;

        public Person(int key) {
            this.__key = key;
            this.name = "Person " + key;
            this.age = key;
            this.height = 100.5 + key;
            this.active = key % 2 == 0;
        }

    }
}
