package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationFuture_ReturnValueTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private InternalOperationService opService;
    private SerializationService serializationService;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        opService = getOperationService(hz);
        serializationService = getSerializationService(hz);
    }

    @Test
    public void whenSerializedInvocation() throws ExecutionException, InterruptedException {
        assertSerializedInvocation(null, null);

        assertSerializedInvocation(1, 1);

        assertSerializedInvocation("foo", "foo");

        assertSerializedInvocation(serializationService.toData("foo"), serializationService.toData("foo"));
    }

    @Test
    public void whenDeserializedInvocation() throws ExecutionException, InterruptedException {
        assertDeserializedInvocation(null, null);

        assertDeserializedInvocation(1, 1);

        assertDeserializedInvocation("foo", "foo");

        assertDeserializedInvocation(serializationService.toData("foo"), "foo");
    }

    private void assertSerializedInvocation(Object input, Object expected) {
        Operation operation = new DummyOperation(input);

        InternalCompletableFuture f =  opService.createInvocationBuilder(null, operation,0)
                .setResultDeserialized(false)
                .invoke();

        try {
            assertEquals(expected, f.get());
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private void assertDeserializedInvocation(Object input, Object expected) {
        Operation operation = new DummyOperation(input);

        InternalCompletableFuture f =  opService.createInvocationBuilder(null, operation,0)
                .setResultDeserialized(true)
                .invoke();

        try {
            assertEquals(expected, f.get());
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

}
