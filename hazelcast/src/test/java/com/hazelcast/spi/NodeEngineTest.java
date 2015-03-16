package com.hazelcast.spi;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class NodeEngineTest extends HazelcastTestSupport {

    private NodeEngineImpl nodeEngine;

    @Before
    public void setUp() {
        HazelcastInstance hz = createHazelcastInstance();
        nodeEngine = getNode(hz).getNodeEngine();
    }

    @Test(expected = NullPointerException.class)
    public void getSharedService_whenNullName() {
        nodeEngine.getSharedService(null);
    }

    @Test
    public void getSharedService_whenNonExistingService() {
        SharedService sharedService = nodeEngine.getSharedService("notexist");
        assertNull(sharedService);
    }

    @Test
    public void getSharedService_whenExistingService() {
        SharedService sharedService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        assertNotNull(sharedService);
        assertTrue(sharedService instanceof LockService);
    }

    @Test
    public void toData_whenNull() {
        Data result = nodeEngine.toData(null);
        assertNull(result);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void toData_whenSerializationProblem() {
        SerializeFailureObject object = new SerializeFailureObject();
        nodeEngine.toData(object);
    }

    public class SerializeFailureObject implements DataSerializable {
        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            throw new RuntimeException();
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    @Test(expected = HazelcastSerializationException.class)
    public void toObject_whenDeserializeProblem() {
        DeserializeFailureObject object = new DeserializeFailureObject();
        Data data = nodeEngine.toData(object);
        nodeEngine.toObject(data);
    }

    @Test
    public void toObject_whenNull() {
        Object actual = nodeEngine.toObject(null);
        assertNull(actual);
    }

    @Test
    public void toObject_whenAlreadyDeserialized() {
        String expected = "foo";
        Object actual = nodeEngine.toObject(expected);
        assertSame(expected, actual);
    }

    public class DeserializeFailureObject implements DataSerializable {
        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            throw new RuntimeException();
        }
    }

    @Test(expected = NullPointerException.class)
    public void getLogger_whenNullString() {
        nodeEngine.getLogger((String) null);
    }

    @Test(expected = NullPointerException.class)
    public void getLogger_whenNullClass() {
        nodeEngine.getLogger((Class) null);
    }
}
