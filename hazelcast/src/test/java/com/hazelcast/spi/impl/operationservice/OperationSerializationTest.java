/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Constructor;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationSerializationTest extends HazelcastTestSupport {

    private static final String DUMMY_SERVICE_NAME = "foobar";

    private InternalSerializationService serializationService;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void test_partitionId() {
        test_partitionId(0, false);
        test_partitionId(100, false);
        test_partitionId(-1, false);
        test_partitionId(Short.MAX_VALUE, false);
        test_partitionId(Short.MAX_VALUE + 1, true);
        test_partitionId(Integer.MAX_VALUE, true);
    }

    private void test_partitionId(int partitionId, boolean is32bit) {
        Operation op = new DummyOperation();
        op.setPartitionId(partitionId);
        assertEquals(partitionId, op.getPartitionId());

        assertEquals("is partition 32 bits", is32bit, op.isFlagSet(Operation.BITMASK_PARTITION_ID_32_BIT));

        assertSerializationCloneEquals(op);
    }

    @Test
    public void test_replicaIndex() {
        test_replicaIndex(0, false);
        test_replicaIndex(1, true);
        test_replicaIndex(3, true);
    }

    private void test_replicaIndex(int replicaIndex, boolean isReplicaIndexSet) {
        Operation op = new DummyOperation();
        op.setReplicaIndex(replicaIndex);
        assertEquals(replicaIndex, op.getReplicaIndex());

        assertEquals("is replicaindex set", isReplicaIndexSet, op.isFlagSet(Operation.BITMASK_REPLICA_INDEX_SET));

        assertSerializationCloneEquals(op);
    }

    @Test
    public void test_callTimeout() {
        test_callTimeout(0, false);
        test_callTimeout(100, false);
        test_callTimeout(-1, false);
        test_callTimeout(Integer.MAX_VALUE, false);
        test_callTimeout(Integer.MAX_VALUE + 1L, true);
        test_callTimeout(Long.MAX_VALUE, true);
    }

    private void test_callTimeout(long callTimeout, boolean callTimeout64Bits) {
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

    private void test_waitTimeout(long waitTimeout, boolean waitTimeoutSet) {
        Operation op = new DummyOperation();
        op.setWaitTimeout(waitTimeout);
        assertEquals(waitTimeout, op.getWaitTimeout());

        assertEquals("wait timeout set", waitTimeoutSet, op.isFlagSet(Operation.BITMASK_WAIT_TIMEOUT_SET));

        assertSerializationCloneEquals(op);
    }

    @Test
    public void test_callerUuid() {
        test_callerUuid(null, false);
        test_callerUuid(UUID.randomUUID(), true);
    }

    private void test_callerUuid(UUID callerUuid, boolean callerUuidSet) {
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

    private void assertSerializationCloneEquals(Operation expected) {
        Operation actual = copy(expected);
        assertEquals("caller UUID does not match", expected.getCallerUuid(), actual.getCallerUuid());
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
    public void test_serviceName_whenOverridesGetServiceName_thenNotSerialized() {
        OperationWithServiceNameOverride op = new OperationWithServiceNameOverride();
        assertNull(op.getRawServiceName());
        assertFalse("service name should not be set", op.isFlagSet(Operation.BITMASK_SERVICE_NAME_SET));

        Operation copy = copy(op);
        assertSame(DUMMY_SERVICE_NAME, copy.getServiceName());
        assertNull(copy.getRawServiceName());
        assertFalse("service name should not be set", copy.isFlagSet(Operation.BITMASK_SERVICE_NAME_SET));
    }

    @Test
    public void test_serviceName_whenNotOverridesServiceName_thenSerialized() {
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

    @Test
    public void test_serviceName_whenOverridesGetServiceNameAndRequiresExplicitServiceName_thenSerialized() {
        OperationWithExplicitServiceNameAndOverride op = new OperationWithExplicitServiceNameAndOverride();
        assertNull(op.getRawServiceName());
        assertFalse("service name should not be set", op.isFlagSet(Operation.BITMASK_SERVICE_NAME_SET));

        Operation copy = copy(op);
        assertEquals(DUMMY_SERVICE_NAME, copy.getRawServiceName());
        assertTrue("service name should be set", copy.isFlagSet(Operation.BITMASK_SERVICE_NAME_SET));
    }

    private Operation copy(Operation op) {
        try {
            BufferObjectDataOutput out = serializationService.createObjectDataOutput(1000);
            op.writeData(out);

            BufferObjectDataInput in = serializationService.createObjectDataInput(out.toByteArray());
            Constructor constructor = op.getClass().getConstructor();
            constructor.setAccessible(true);
            Operation copiedOperation = (Operation) constructor.newInstance();
            copiedOperation.readData(in);
            return copiedOperation;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void assertCopy(String expected, String actual) {
        assertEquals(expected, actual);
        assertNotSame(expected, actual);
    }

    private static class DummyOperation extends Operation {

        @SuppressWarnings("checkstyle:RedundantModifier")
        public DummyOperation() {
        }

        @Override
        public void run() throws Exception {

        }
    }

    private static class OperationWithServiceNameOverride extends Operation {

        @SuppressWarnings("checkstyle:RedundantModifier")
        public OperationWithServiceNameOverride() {
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        public String getServiceName() {
            return DUMMY_SERVICE_NAME;
        }
    }

    private static class OperationWithExplicitServiceNameAndOverride extends Operation {

        @SuppressWarnings("checkstyle:RedundantModifier")
        public OperationWithExplicitServiceNameAndOverride() {
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        protected boolean requiresExplicitServiceName() {
            return true;
        }

        @Override
        public String getServiceName() {
            return DUMMY_SERVICE_NAME;
        }

    }

}
