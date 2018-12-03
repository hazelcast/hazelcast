package com.hazelcast.query.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GenericCollectionQueriesTest extends HazelcastTestSupport {

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        MapConfig mapConfig = config.getMapConfig("map");
        mapConfig.addMapIndexConfig(new MapIndexConfig("children[any].id", true));
//        mapConfig.addMapIndexConfig(new MapIndexConfig("childrenMethod[any].id", true));
        mapConfig.addMapIndexConfig(new MapIndexConfig("childrenList[any].id", true));
//        mapConfig.addMapIndexConfig(new MapIndexConfig("childrenListMethod[any].id", true));
        return config;
    }

    @Test
    public void testList() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Integer, Parent> map = instance.getMap("map");

        Parent parent = new Parent();
        parent.children.add(new Child1(0));
        parent.children.add(new Child2(1));
        parent.children.add(new Child1(2));

        parent.childrenList.add(new Child1(0));
        parent.childrenList.add(new Child2(1));
        parent.childrenList.add(new Child1(2));
        map.put(0, parent);

        Collection<Parent> result;

        result = map.values(Predicates.greaterEqual("children[any].id", 1));
        Assert.assertEquals(1, result.size());

//        result = map.values(Predicates.greaterEqual("childrenMethod[any].id", 1));
//        Assert.assertEquals(1, result.size());

        result = map.values(Predicates.greaterEqual("childrenList[any].id", 1));
        Assert.assertEquals(1, result.size());
    }

    public static class Parent implements Serializable {
        public final List<Child> children = new ArrayList<Child>();

        public final ChildrenList childrenList = new ChildrenList();

        public List<Child> childrenMethod() {
            return children;
        }

        public List<Child> childrenListMethod() {
            return childrenList;
        }
    }

    public interface Child extends Serializable {
        int getId();
    }

    public static class Child1 implements Child {
        private final int id;

        public Child1(int id) {
            this.id = id;
        }

        @Override
        public int getId() {
            return id;
        }
    }

    public static class Child2 implements Child {
        private final int id;

        public Child2(int id) {
            this.id = id;
        }

        @Override
        public int getId() {
            return id;
        }
    }

    public static class ChildrenList extends ArrayList<Child> {
    }

}
