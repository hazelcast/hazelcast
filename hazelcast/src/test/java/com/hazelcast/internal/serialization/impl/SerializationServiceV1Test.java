package com.hazelcast.internal.serialization.impl;

import com.hazelcast.executor.impl.operations.CancellationOperation;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.test.AbstractTestOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SerializationServiceV1Test {

    private SerializationServiceV1 serializationService;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        DefaultSerializationServiceBuilder defaultSerializationServiceBuilder = new DefaultSerializationServiceBuilder();
        serializationService = defaultSerializationServiceBuilder
                .setVersion(InternalSerializationService.VERSION_1).build();
    }

    @Test
    public void test_callid_on_correct_stream_position() throws Exception {
        CancellationOperation operation = new CancellationOperation(UuidUtil.newUnsecureUuidString(), true);
        operation.setCallerUuid(UuidUtil.newUnsecureUuidString());
        OperationAccessor.setCallId(operation, 12345);

        Data data = serializationService.toData(operation);
        long callId = serializationService.initDataSerializableInputAndSkipTheHeader(data).readLong();

        assertEquals(12345, callId);
    }

    @Test
    public void testExtractOperationCallId() throws Exception {
        IoUtilTestOperation operation = new IoUtilTestOperation(1);
        OperationAccessor.setCallId(operation, 2342);
        Data data = serializationService.toData(operation);

        long callId = serializationService.initDataSerializableInputAndSkipTheHeader(data).readLong();

        assertEquals(2342, callId);
    }

    @Test
    public void testExtractOperationCallId_withIdentifiedOperation() throws Exception {
        IdentifiedIoUtilTestOperation operation = new IdentifiedIoUtilTestOperation(1);
        OperationAccessor.setCallId(operation, 4223);
        Data data = serializationService.toData(operation);

        long callId = serializationService.initDataSerializableInputAndSkipTheHeader(data).readLong();

        assertEquals(4223, callId);
    }

    private static class IoUtilTestOperation extends AbstractTestOperation {

        IoUtilTestOperation(int partitionId) {
            super(partitionId);
        }

        @Override
        protected Object doRun() {
            return null;
        }
    }

    private static class IdentifiedIoUtilTestOperation extends IoUtilTestOperation implements IdentifiedDataSerializable {

        IdentifiedIoUtilTestOperation(int partitionId) {
            super(partitionId);
        }

        @Override
        public int getFactoryId() {
            return 23;
        }

        @Override
        public int getId() {
            return 42;
        }
    }


}
