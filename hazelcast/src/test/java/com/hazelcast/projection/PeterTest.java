package com.hazelcast.projection;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collection;

public class PeterTest {

    @Test
    public void test() {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig("persons");
        config.addMapConfig(mapConfig);
        mapConfig.addMapIndexConfig(new MapIndexConfig("age", false));
        mapConfig.addMapIndexConfig(new MapIndexConfig("iq", false));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        IMap map = hz.getMap("persons");
        for (int k = 0; k < 100; k++) {
            Person person = new Person(k, 100);
            map.put(k, person);
        }

        SqlPredicate sqlPredicate = new SqlPredicate("age=10 and %iq=100 and money=0");
        System.out.println(sqlPredicate.getPredicate());

        Collection result = map.values(sqlPredicate);
        System.out.println("result.size:"+result);
//        System.out.println("equal predicate comparisons:"+ EqualPredicate.comparison);
//        System.out.println("filtered items:"+ EqualPredicate.filtered);
        System.out.println(result);
    }

    public static class Person implements Serializable {
        private  int iq;
        private int age;
        private int money;

        public Person(int age, int iq) {
            this.age = age;
            this.iq = iq;
        }

        public int getIq() {
            return iq;
        }

        public int getAge() {
            return age;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "age=" + age +
                    ", iq=" + iq +
                    '}';
        }
    }
}