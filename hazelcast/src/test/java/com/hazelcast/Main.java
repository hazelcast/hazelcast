package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastTestSupport;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by alarmnummer on 9/14/15.
 */
public class Main extends HazelcastTestSupport {

    public static void main(String[] args){
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
        IMap map = hz1.getMap("foo");


        map.put(generateKeyOwnedBy(hz2), new Value());
        map.put(generateKeyOwnedBy(hz2), new Value());
        map.put(generateKeyOwnedBy(hz2), new Value());
        map.put(generateKeyOwnedBy(hz2), new Value());

        System.out.println("item placed");

        System.out.println("executing query");
        Collection<String> keys = map.keySet(new MyPredicate());
        System.out.println("Printing keys");
        for(String key:keys){
            System.out.println(key);
        }
    }

    static class MyPredicate implements Predicate {
        @Override
        public boolean apply(Map.Entry mapEntry) {
            return true;
        }
    }

    static class Key implements DataSerializable{
        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            System.out.println("key serialize");
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            System.out.println("key deserialize");
        }
    }

    static class Value implements DataSerializable{
        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            System.out.println("value serialize");
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            System.out.println("value deserialize");
        }
    }
}
