package com.hazelcast.spi;

import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationSerializationTest extends HazelcastTestSupport {

    private SerializationService serializationService;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void test_partitionId() throws IOException {
        test_partitionId(0, false);
        test_partitionId(100, false);
        test_partitionId(-1, false);
        test_partitionId(Short.MAX_VALUE, false);
        test_partitionId(Short.MAX_VALUE + 1, true);
        test_partitionId(Integer.MAX_VALUE, true);
    }

    public void test_partitionId(int partitionId, boolean is32bit) {
        Operation op = new DummyOperation();
        op.setPartitionId(partitionId);
        assertEquals(partitionId, op.getPartitionId());

        assertEquals("is partition 32 bits", is32bit, op.isFlagSet(Operation.BITMASK_PARTITION_ID_32_BIT));

        assertSerializationCloneEquals(op);
    }

    @Test
    public void test_replicaIndex() throws IOException {
        test_replicaIndex(0, false);
        test_replicaIndex(1, true);
        test_replicaIndex(3, true);
    }

    public void test_replicaIndex(int replicaIndex, boolean isReplicaIndexSet) {
        Operation op = new DummyOperation();
        op.setReplicaIndex(replicaIndex);
        assertEquals(replicaIndex, op.getReplicaIndex());

        assertEquals("is replicaindex set", isReplicaIndexSet, op.isFlagSet(Operation.BITMASK_REPLICA_INDEX_SET));

        assertSerializationCloneEquals(op);
    }

    @Test
    public void test_callTimeout() throws IOException {
        test_callTimeout(0, false);
        test_callTimeout(100, false);
        test_callTimeout(-1, false);
        test_callTimeout(Integer.MAX_VALUE, false);
        test_callTimeout(Integer.MAX_VALUE + 1l, true);
        test_callTimeout(Long.MAX_VALUE, true);
    }

    public void test_callTimeout(long callTimeout, boolean callTimeout64Bits) {
        Operation op = new DummyOperation();
        op.setCallTimeout(callTimeout);
        assertEquals(callTimeout, op.getCallTimeout());

        assertEquals("is calltimeout 64 bits", callTimeout64Bits, op.isFlagSet(Operation.BITMASK_CALL_TIMEOUT_64_BIT));

        assertSerializationCloneEquals(op);
    }

    @Test
    public void test_callId() {
        Operation op = new DummyOperation();
        op.setCallId(10000);
        assertEquals(10000, op.getCallId());
        assertSerializationCloneEquals(op);
    }

    @Test
    public void test_invocationTime() {
        Operation op = new DummyOperation();
        op.setInvocationTime(10000);
        assertEquals(10000, op.getInvocationTime());
        assertSerializationCloneEquals(op);
    }

    @Test
    public void test_waitTimeout() {
        test_waitTimeout(-1, false);
        test_waitTimeout(0, true);
        test_waitTimeout(1, true);
    }

    public void test_waitTimeout(long waitTimeout, boolean waitTimeoutSet) {
        Operation op = new DummyOperation();
        op.setWaitTimeout(waitTimeout);
        assertEquals(waitTimeout, op.getWaitTimeout());

        assertEquals("wait timeout set", waitTimeoutSet, op.isFlagSet(Operation.BITMASK_WAIT_TIMEOUT_SET));

        assertSerializationCloneEquals(op);
    }

    @Test
    public void test_callerUuid() {
        test_callerUuid(null, false);
        test_callerUuid("", true);
        test_callerUuid("foofbar", true);
    }

    public void test_callerUuid(String callerUuid, boolean callerUuidSet) {
        Operation op = new DummyOperation();
        op.setCallerUuid(callerUuid);
        assertEquals(callerUuid, op.getCallerUuid());

        assertEquals("wait timeout set", callerUuidSet, op.isFlagSet(Operation.BITMASK_CALLER_UUID_SET));

        assertSerializationCloneEquals(op);
    }

    public void assertSerializationCloneEquals(Operation expected) {
        Operation actual = copy(expected);
        assertEquals("caller uuid does not match", expected.getCallerUuid(), actual.getCallerUuid());
        assertEquals("call timeout does not match", expected.getCallTimeout(), actual.getCallTimeout());
        assertEquals("validates target does not match", expected.validatesTarget(), actual.validatesTarget());
        assertEquals("callid does not match", expected.getCallId(), actual.getCallId());
        assertEquals("invocation time does not match", expected.getInvocationTime(), actual.getInvocationTime());
        assertEquals("partitionId does not match", expected.getPartitionId(), actual.getPartitionId());
        assertEquals("replica index does not match", expected.getReplicaIndex(), actual.getReplicaIndex());
        assertEquals("state does not match", expected.getFlags(), actual.getFlags());
        assertEquals("wait timeout does not match", expected.getWaitTimeout(), actual.getWaitTimeout());
    }

    private Operation copy(Operation op) {
        try {
            BufferObjectDataOutput out = serializationService.createObjectDataOutput(1000);
            op.writeData(out);

            BufferObjectDataInput in = serializationService.createObjectDataInput(out.toByteArray());
            Operation copiedOperation = new DummyOperation();
            copiedOperation.readData(in);
            return copiedOperation;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class DummyOperation extends AbstractOperation {
        @Override
        public void run() throws Exception {
        }
    }
}
