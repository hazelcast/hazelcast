package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AtomicReferenceMigrationTest extends HazelcastTestSupport {

    @Test
    public void testWhenInstancesShutdown() {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        IAtomicReference<SimpleObject> reference1 = instance1.getAtomicReference("test");
        SimpleObject object = new SimpleObject(1);
        reference1.set(object);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        instance1.shutdown();
        IAtomicReference<SimpleObject> reference2 = instance2.getAtomicReference("test");
        SimpleObject objectTest1 = reference2.get();
        assertEquals(object, objectTest1);
    }

    @Test
    public void testMultipleAtomicReferences() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        SimpleObject object = new SimpleObject(1);
        for (int i = 0; i < 100; i++) {
            IAtomicReference<SimpleObject> reference = instance1.getAtomicReference("test" + i);
            reference.set(object);
        }
        HazelcastInstance instance2 = factory.newHazelcastInstance();
        warmUpPartitions(instance1, instance2);
        for (int i = 0; i < 100; i++) {
            IAtomicReference<SimpleObject> reference = instance2.getAtomicReference("test" + i);
            assertEquals(object, reference.get());
        }
        HazelcastInstance instance3 = factory.newHazelcastInstance();
        warmUpPartitions(instance1, instance2, instance3);
        for (int i = 0; i < 100; i++) {
            IAtomicReference<SimpleObject> reference = instance3.getAtomicReference("test" + i);
            assertEquals(object, reference.get());
        }
    }

    static class SimpleObject implements DataSerializable, Serializable {
        int field;

        SimpleObject(int field) {
            this.field = field;
        }

        SimpleObject() {
        }

        public int getField() {
            return field;
        }

        public void setField(int field) {
            this.field = field;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(field);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            field = in.readInt();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SimpleObject that = (SimpleObject) o;

            if (field != that.field) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return field;
        }
    }
}
