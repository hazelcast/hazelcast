package com.hazelcast.map.impl.operation;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapFetchIndexOperationTest extends HazelcastTestSupport {

    @Test
    public void testNoMigration() {
        String mapName = "map1";

        Config config = smallInstanceConfig();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);

        IMap<String, Person> map = h1.getMap(mapName);

        map.addIndex(IndexType.SORTED, "age");

        map.put("person1", new Person("person1", 45));
        map.put("person2", new Person("person2", 39));

        Person p = map.get("person2");

        assertEquals(39, p.getAge());
        assertEquals("person2", p.getName());

        getOperationService(h1);
    }

    static class Person implements Serializable {
        private String name;
        private int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() { return this.name; }

        public int getAge() { return this.age; }

        public void setName(String name) { this.name = name; }

        public void setAge(int age) { this.age = age; }
    }
}
