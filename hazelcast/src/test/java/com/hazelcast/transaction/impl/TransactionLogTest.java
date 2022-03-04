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

package com.hazelcast.transaction.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransactionLogTest {

    @Test
    public void add_whenKeyAware() {
        TransactionLog log = new TransactionLog();
        TransactionLogRecord record = mock(TransactionLogRecord.class);
        String key = "foo";
        when(record.getKey()).thenReturn(key);

        log.add(record);

        assertSame(record, log.get(key));
        assertEquals(1, log.size());
    }

    @Test
    public void add_whenNotKeyAware() {
        TransactionLog log = new TransactionLog();
        TransactionLogRecord record = mock(TransactionLogRecord.class);

        log.add(record);

        assertEquals(1, log.size());
        assertThat(log.getRecords(), contains(record));
    }

    @Test
    public void add_whenOverwrite() {
        TransactionLog log = new TransactionLog();
        String key = "foo";
        // first we insert the old record
        TransactionLogRecord oldRecord = mock(TransactionLogRecord.class);
        when(oldRecord.getKey()).thenReturn(key);
        log.add(oldRecord);

        // then we insert the old record
        TransactionLogRecord newRecord = mock(TransactionLogRecord.class);
        when(newRecord.getKey()).thenReturn(key);
        log.add(newRecord);

        assertSame(newRecord, log.get(key));
        assertEquals(1, log.size());
    }

    @Test
    public void remove_whenNotExist_thenCallIgnored() {
        TransactionLog log = new TransactionLog();
        log.remove("not exist");
    }

    @Test
    public void remove_whenExist_thenRemoved() {
        TransactionLog log = new TransactionLog();
        TransactionLogRecord record = mock(TransactionLogRecord.class);
        String key = "foo";
        when(record.getKey()).thenReturn(key);
        log.add(record);

        log.remove(key);

        assertNull(log.get(key));
    }

    @Test
    public void prepare_partitionSpecificRecord() throws Exception {
        OperationService operationService = mock(OperationService.class);
        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getOperationService()).thenReturn(operationService);

        TransactionLog log = new TransactionLog();

        TransactionLogRecord partitionRecord = mock(TransactionLogRecord.class);
        Operation partitionOperation = new DummyPartitionOperation();
        when(partitionRecord.newPrepareOperation()).thenReturn(partitionOperation);

        log.add(partitionRecord);
        log.prepare(nodeEngine);

        verify(operationService, times(1))
                .invokeOnPartition(partitionOperation.getServiceName(), partitionOperation,
                        partitionOperation.getPartitionId());
    }

    @Test
    public void rollback_partitionSpecificRecord() throws Exception {
        OperationService operationService = mock(OperationService.class);
        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getOperationService()).thenReturn(operationService);

        TransactionLog log = new TransactionLog();

        TransactionLogRecord partitionRecord = mock(TransactionLogRecord.class);
        Operation partitionOperation = new DummyPartitionOperation();
        when(partitionRecord.newRollbackOperation()).thenReturn(partitionOperation);

        log.add(partitionRecord);
        log.rollback(nodeEngine);

        verify(operationService, times(1))
                .invokeOnPartition(partitionOperation.getServiceName(),
                        partitionOperation, partitionOperation.getPartitionId());
    }

    @Test
    public void commit_partitionSpecificRecord() throws Exception {
        OperationService operationService = mock(OperationService.class);
        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getOperationService()).thenReturn(operationService);

        TransactionLog log = new TransactionLog();

        TransactionLogRecord partitionRecord = mock(TransactionLogRecord.class);
        Operation partitionOperation = new DummyPartitionOperation();
        when(partitionRecord.newCommitOperation()).thenReturn(partitionOperation);

        log.add(partitionRecord);
        log.commit(nodeEngine);

        verify(operationService, times(1))
                .invokeOnPartition(partitionOperation.getServiceName(), partitionOperation,
                        partitionOperation.getPartitionId());
    }

    @Test
    public void prepare_targetAwareRecord() throws Exception {
        OperationService operationService = mock(OperationService.class);
        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getOperationService()).thenReturn(operationService);

        TransactionLog log = new TransactionLog();

        Address target = new Address(InetAddress.getLocalHost(), 5000);
        TargetAwareTransactionLogRecord targetRecord = mock(TargetAwareTransactionLogRecord.class);
        when(targetRecord.getTarget()).thenReturn(target);

        DummyTargetOperation targetOperation = new DummyTargetOperation();
        when(targetRecord.newPrepareOperation()).thenReturn(targetOperation);

        log.add(targetRecord);
        log.prepare(nodeEngine);

        verify(operationService, times(1))
                .invokeOnTarget(targetOperation.getServiceName(), targetOperation, target);
    }

    @Test
    public void rollback_targetAwareRecord() throws Exception {
        OperationService operationService = mock(OperationService.class);
        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getOperationService()).thenReturn(operationService);

        TransactionLog log = new TransactionLog();

        Address target = new Address(InetAddress.getLocalHost(), 5000);
        TargetAwareTransactionLogRecord targetRecord = mock(TargetAwareTransactionLogRecord.class);
        when(targetRecord.getTarget()).thenReturn(target);

        DummyTargetOperation targetOperation = new DummyTargetOperation();
        when(targetRecord.newRollbackOperation()).thenReturn(targetOperation);

        log.add(targetRecord);
        log.rollback(nodeEngine);

        verify(operationService, times(1))
                .invokeOnTarget(targetOperation.getServiceName(), targetOperation, target);
    }

    @Test
    public void commit_targetAwareRecord() throws Exception {
        OperationService operationService = mock(OperationService.class);
        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getOperationService()).thenReturn(operationService);

        TransactionLog log = new TransactionLog();

        Address target = new Address(InetAddress.getLocalHost(), 5000);
        TargetAwareTransactionLogRecord targetRecord = mock(TargetAwareTransactionLogRecord.class);
        when(targetRecord.getTarget()).thenReturn(target);

        DummyTargetOperation targetOperation = new DummyTargetOperation();
        when(targetRecord.newCommitOperation()).thenReturn(targetOperation);

        log.add(targetRecord);
        log.commit(nodeEngine);

        verify(operationService, times(1))
                .invokeOnTarget(targetOperation.getServiceName(), targetOperation, target);
    }

    private static class DummyPartitionOperation extends Operation {
        {
            setPartitionId(0);
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        public String getServiceName() {
            return "dummy";
        }
    }

    private static class DummyTargetOperation extends Operation {
        @Override
        public void run() throws Exception {
        }

        @Override
        public String getServiceName() {
            return "dummy";
        }
    }
}
