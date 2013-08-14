package com.hazelcast.nio.serialization;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class SerializersHazelcastInstanceAwareTest extends HazelcastTestSupport {

    @Test
    public void testPortableFactoryInstance(){
        HazelcastInstanceAwarePortableFactory factory = new HazelcastInstanceAwarePortableFactory();

        Config config = new Config();
        config.getSerializationConfig().addPortableFactory(1, factory);

        HazelcastInstance instance = createHazelcastInstanceFactory(1).newHazelcastInstance(config);
        Map<String,PortablePerson> map = instance.getMap("map");
        map.put("1", new PortablePerson());
        PortablePerson person = map.get("1");
        assertNotNull("HazelcastInstance should have been set",person.hz);
    }

    @Test
    public void testPortableFactoryClass(){
        Config config = new Config();
        config.getSerializationConfig().addPortableFactoryClass(1, HazelcastInstanceAwarePortableFactory.class.getName());

        HazelcastInstance instance = createHazelcastInstanceFactory(1).newHazelcastInstance(config);
        Map<String,PortablePerson> map = instance.getMap("map");
        map.put("1", new PortablePerson());
        PortablePerson person = map.get("1");
        assertNotNull("HazelcastInstance should have been set",person.hz);
    }

    @Test
    public void testDataSerializableFactoryInstance(){
        HazelcastInstanceAwareDataSerializableFactory factory = new HazelcastInstanceAwareDataSerializableFactory();

        Config config = new Config();
        config.getSerializationConfig().addDataSerializableFactory(1, factory);

        HazelcastInstance instance = createHazelcastInstanceFactory(1).newHazelcastInstance(config);
        Map<String,DataSerializablePerson> map = instance.getMap("map");
        map.put("1", new DataSerializablePerson());
        DataSerializablePerson person = map.get("1");
        assertNotNull("HazelcastInstance should have been set",person.hz);
    }

    @Test
    public void testDataSerializableFactoryClass(){
        Config config = new Config();
        config.getSerializationConfig().addDataSerializableFactoryClass(1, HazelcastInstanceAwareDataSerializableFactory.class.getName());

        HazelcastInstance instance = createHazelcastInstanceFactory(1).newHazelcastInstance(config);
        Map<String,DataSerializablePerson> map = instance.getMap("map");
        map.put("1", new DataSerializablePerson());
        DataSerializablePerson person = map.get("1");
        assertNotNull("HazelcastInstance should have been set",person.hz);
    }

    private static class HazelcastInstanceAwarePortableFactory implements PortableFactory, HazelcastInstanceAware {
        private HazelcastInstance hz;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
           this.hz = hazelcastInstance;
        }

        @Override
        public Portable create(int classId) {
            PortablePerson p = new PortablePerson();
            p.hz = hz;
            return p;
        }
    }

    private static class PortablePerson implements Portable{
        private HazelcastInstance hz;

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
        }
    }

    private static class HazelcastInstanceAwareDataSerializableFactory implements DataSerializableFactory, HazelcastInstanceAware {
        private HazelcastInstance hz;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            DataSerializablePerson p = new DataSerializablePerson();
            p.hz = hz;
            return p;
        }
    }

    private static class DataSerializablePerson implements IdentifiedDataSerializable{
        private HazelcastInstance hz;

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getId() {
            return 1;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }

}
