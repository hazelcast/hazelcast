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
import java.lang.reflect.Constructor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationSerializationTest extends HazelcastTestSupport {

    public static final String DUMMY_SERVICE_NAME = "foobar";

    private SerializationService serializationService;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void test_syncForced(){
        DummyOperation op = new DummyOperation();
        assertFalse(op.isSyncForced());

        OperationAccessor.setForceSync(op, true);
        assertTrue(op.isSyncForced());

        OperationAccessor.setForceSync(op, false);
        assertFalse(op.isSyncForced());
    }

    @Test
    public void test_validateTarget(){
        DummyOperation op = new DummyOperation();
        assertTrue(op.validatesTarget());

        op.setValidateTarget(true);
        assertTrue(op.validatesTarget());

        op.setValidateTarget(false);
        assertFalse(op.validatesTarget());
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

    @Test
    public void test_validateTarget_defaultValue() {
        Operation op = new DummyOperation();

        assertTrue("Default value of validate target should be TRUE", op.validatesTarget());
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

    @Test
    public void test_serviceName_whenOverridesGetServiceName_thenNotSerialized(){
        OperationWithServiceNameOverride op = new OperationWithServiceNameOverride();
        assertNull(op.getRawServiceName());
        assertFalse("service name should not be set", op.isFlagSet(Operation.BITMASK_SERVICE_NAME_SET));

        Operation copy = copy(op);
        assertSame(DUMMY_SERVICE_NAME, copy.getServiceName());
        assertNull(copy.getRawServiceName());
        assertFalse("service name should not be set", copy.isFlagSet(Operation.BITMASK_SERVICE_NAME_SET));
    }

    @Test
    public void test_serviceName_whenNotOverridesServiceName_thenSerialized(){
        DummyOperation op = new DummyOperation();
        op.setServiceName(DUMMY_SERVICE_NAME);
        assertSame(DUMMY_SERVICE_NAME, op.getRawServiceName());
        assertSame(DUMMY_SERVICE_NAME, op.getServiceName());
        assertTrue("service name should be set", op.isFlagSet(Operation.BITMASK_SERVICE_NAME_SET));

        Operation copy = copy(op);
        assertCopy(DUMMY_SERVICE_NAME, copy.getServiceName());
        assertCopy(DUMMY_SERVICE_NAME, copy.getRawServiceName());
        assertTrue("service name should be set", copy.isFlagSet(Operation.BITMASK_SERVICE_NAME_SET));
    }

    public void assertCopy(String expected, String actual){
        assertEquals(expected, actual);
        assertNotSame(expected, actual);
    }

    private Operation copy(Operation op) {
        try {
            BufferObjectDataOutput out = serializationService.createObjectDataOutput(1000);
            op.writeData(out);

            BufferObjectDataInput in = serializationService.createObjectDataInput(out.toByteArray());
            Constructor constructor = op.getClass().getConstructor();
            constructor.setAccessible(true);
            Operation copiedOperation = (Operation)constructor.newInstance();
            copiedOperation.readData(in);
            return copiedOperation;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class DummyOperation extends AbstractOperation {
        public DummyOperation(){}
        @Override
        public void run() throws Exception {

        }
    }

    private static class OperationWithServiceNameOverride extends AbstractOperation {
        public OperationWithServiceNameOverride(){}

        @Override
        public void run() throws Exception {
        }

        @Override
        public String getServiceName() {
            return DUMMY_SERVICE_NAME;
        }
    }
}
