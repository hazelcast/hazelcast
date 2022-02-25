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

package com.hazelcast.osgi;

import com.hazelcast.client.ClientService;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Endpoint;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.osgi.impl.HazelcastOSGiTestUtil.createHazelcastOSGiInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastOSGiInstanceTest {

    @Test
    @SuppressWarnings("EqualsWithItself")
    public void equalsReturnsTrueForSameOSGiInstances() {
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        assertTrue(hazelcastOSGiInstance.equals(hazelcastOSGiInstance));
    }

    @Test
    @SuppressWarnings("ObjectEqualsNull")
    public void equalsReturnsFalseForNullObject() {
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        assertFalse(hazelcastOSGiInstance.equals(null));
    }

    @Test
    public void equalsReturnsFalseForDifferentTypedObject() {
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        assertFalse(hazelcastOSGiInstance.equals(new Object()));
    }

    @Test
    public void equalsReturnsFalseForDifferentOSGiInstancesWithDifferentDelegatedInstanceAndSameService() {
        HazelcastInstance mockHazelcastInstance1 = mock(HazelcastInstance.class);
        HazelcastInstance mockHazelcastInstance2 = mock(HazelcastInstance.class);
        HazelcastOSGiService mockService = mock(HazelcastOSGiService.class);
        HazelcastOSGiInstance hazelcastOSGiInstance1 = createHazelcastOSGiInstance(mockHazelcastInstance1, mockService);
        HazelcastOSGiInstance hazelcastOSGiInstance2 = createHazelcastOSGiInstance(mockHazelcastInstance2, mockService);

        assertFalse(hazelcastOSGiInstance1.equals(hazelcastOSGiInstance2));
    }

    @Test
    public void equalsReturnsFalseForDifferentOSGiInstancesWithSameDelegatedInstanceAndDifferentService() {
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiService mockService1 = mock(HazelcastOSGiService.class);
        HazelcastOSGiService mockService2 = mock(HazelcastOSGiService.class);
        HazelcastOSGiInstance hazelcastOSGiInstance1 = createHazelcastOSGiInstance(mockHazelcastInstance, mockService1);
        HazelcastOSGiInstance hazelcastOSGiInstance2 = createHazelcastOSGiInstance(mockHazelcastInstance, mockService2);

        assertFalse(hazelcastOSGiInstance1.equals(hazelcastOSGiInstance2));
    }

    @Test
    public void equalsReturnsFalseForDifferentOSGiInstancesWithDifferentDelegatedInstanceAndDifferentService() {
        HazelcastInstance mockHazelcastInstance1 = mock(HazelcastInstance.class);
        HazelcastInstance mockHazelcastInstance2 = mock(HazelcastInstance.class);
        HazelcastOSGiService mockService1 = mock(HazelcastOSGiService.class);
        HazelcastOSGiService mockService2 = mock(HazelcastOSGiService.class);
        HazelcastOSGiInstance hazelcastOSGiInstance1 = createHazelcastOSGiInstance(mockHazelcastInstance1, mockService1);
        HazelcastOSGiInstance hazelcastOSGiInstance2 = createHazelcastOSGiInstance(mockHazelcastInstance2, mockService2);

        assertFalse(hazelcastOSGiInstance1.equals(hazelcastOSGiInstance2));
    }

    @Test
    public void equalsReturnsTrueForDifferentOSGiInstancesWithSameDelegatedInstanceAndSameService() {
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiService mockService = mock(HazelcastOSGiService.class);
        HazelcastOSGiInstance hazelcastOSGiInstance1 = createHazelcastOSGiInstance(mockHazelcastInstance, mockService);
        HazelcastOSGiInstance hazelcastOSGiInstance2 = createHazelcastOSGiInstance(mockHazelcastInstance, mockService);

        assertTrue(hazelcastOSGiInstance1.equals(hazelcastOSGiInstance2));
    }

    @Test
    public void getDelegatedInstanceCalledSuccessfullyOverOSGiInstance() {
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        assertEquals(mockHazelcastInstance, hazelcastOSGiInstance.getDelegatedInstance());
    }

    @Test
    public void getOwnerServiceCalledSuccessfullyOverOSGiInstance() {
        HazelcastOSGiService mockHazelcastOSGiService = mock(HazelcastOSGiService.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastOSGiService);

        assertEquals(mockHazelcastOSGiService, hazelcastOSGiInstance.getOwnerService());
    }

    @Test
    public void getNameCalledSuccessfullyOverOSGiInstance() {
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getName()).thenReturn("my-name");

        assertEquals("my-name", hazelcastOSGiInstance.getName());

        verify(mockHazelcastInstance).getName();
    }

    @Test
    public void getQueueCalledSuccessfullyOverOSGiInstance() {
        IQueue<Object> mockQueue = mock(IQueue.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getQueue("my-queue")).thenReturn(mockQueue);

        assertEquals(mockQueue, hazelcastOSGiInstance.getQueue("my-queue"));

        verify(mockHazelcastInstance).getQueue("my-queue");
    }

    @Test
    public void getTopicCalledSuccessfullyOverOSGiInstance() {
        ITopic<Object> mockTopic = mock(ITopic.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getTopic("my-topic")).thenReturn(mockTopic);

        assertEquals(mockTopic, hazelcastOSGiInstance.getTopic("my-topic"));

        verify(mockHazelcastInstance).getTopic("my-topic");
    }

    @Test
    public void getSetCalledSuccessfullyOverOSGiInstance() {
        ISet<Object> mockSet = mock(ISet.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getSet("my-set")).thenReturn(mockSet);

        assertEquals(mockSet, hazelcastOSGiInstance.getSet("my-set"));

        verify(mockHazelcastInstance).getSet("my-set");
    }

    @Test
    public void getListCalledSuccessfullyOverOSGiInstance() {
        IList<Object> mockList = mock(IList.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getList("my-list")).thenReturn(mockList);

        assertEquals(mockList, hazelcastOSGiInstance.getList("my-list"));

        verify(mockHazelcastInstance).getList("my-list");
    }

    @Test
    public void getMapCalledSuccessfullyOverOSGiInstance() {
        IMap<Object, Object> mockMap = mock(IMap.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getMap("my-map")).thenReturn(mockMap);

        assertEquals(mockMap, hazelcastOSGiInstance.getMap("my-map"));

        verify(mockHazelcastInstance).getMap("my-map");
    }

    @Test
    public void getReplicatedMapCalledSuccessfullyOverOSGiInstance() {
        ReplicatedMap<Object, Object> mockReplicatedMap = mock(ReplicatedMap.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getReplicatedMap("my-replicatedmap")).thenReturn(mockReplicatedMap);

        assertEquals(mockReplicatedMap, hazelcastOSGiInstance.getReplicatedMap("my-replicatedmap"));

        verify(mockHazelcastInstance).getReplicatedMap("my-replicatedmap");
    }

    @Test
    public void getMultiMapCalledSuccessfullyOverOSGiInstance() {
        MultiMap<Object, Object> mockMultiMap = mock(MultiMap.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getMultiMap("my-multimap")).thenReturn(mockMultiMap);

        assertEquals(mockMultiMap, hazelcastOSGiInstance.getMultiMap("my-multimap"));

        verify(mockHazelcastInstance).getMultiMap("my-multimap");
    }

    @Test
    public void getLockCalledSuccessfullyOverOSGiInstance() {
        FencedLock mockLock = mock(FencedLock.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        CPSubsystem cpSubsystem = mock(CPSubsystem.class);
        when(mockHazelcastInstance.getCPSubsystem()).thenReturn(cpSubsystem);

        when(cpSubsystem.getLock("my-lock")).thenReturn(mockLock);

        assertEquals(mockLock, hazelcastOSGiInstance.getCPSubsystem().getLock("my-lock"));

        verify(cpSubsystem).getLock("my-lock");
    }

    @Test
    public void getRingbufferCalledSuccessfullyOverOSGiInstance() {
        Ringbuffer<Object> mockRingbuffer = mock(Ringbuffer.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getRingbuffer("my-ringbuffer")).thenReturn(mockRingbuffer);

        assertEquals(mockRingbuffer, hazelcastOSGiInstance.getRingbuffer("my-ringbuffer"));

        verify(mockHazelcastInstance).getRingbuffer("my-ringbuffer");
    }

    @Test
    public void getReliableTopicCalledSuccessfullyOverOSGiInstance() {
        ITopic<Object> mockReliableTopic = mock(ITopic.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getReliableTopic("my-reliabletopic")).thenReturn(mockReliableTopic);

        assertEquals(mockReliableTopic, hazelcastOSGiInstance.getReliableTopic("my-reliabletopic"));

        verify(mockHazelcastInstance).getReliableTopic("my-reliabletopic");
    }

    @Test
    public void getClusterCalledSuccessfullyOverOSGiInstance() {
        Cluster mockCluster = mock(Cluster.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getCluster()).thenReturn(mockCluster);

        assertEquals(mockCluster, hazelcastOSGiInstance.getCluster());

        verify(mockHazelcastInstance).getCluster();
    }

    @Test
    public void getLocalEndpointCalledSuccessfullyOverOSGiInstance() {
        Endpoint mockEndpoint = mock(Endpoint.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getLocalEndpoint()).thenReturn(mockEndpoint);

        assertEquals(mockEndpoint, hazelcastOSGiInstance.getLocalEndpoint());

        verify(mockHazelcastInstance).getLocalEndpoint();
    }

    @Test
    public void getExecutorServiceCalledSuccessfullyOverOSGiInstance() {
        IExecutorService mockExecutorService = mock(IExecutorService.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getExecutorService("my-executorservice")).thenReturn(mockExecutorService);

        assertEquals(mockExecutorService, hazelcastOSGiInstance.getExecutorService("my-executorservice"));

        verify(mockHazelcastInstance).getExecutorService("my-executorservice");
    }

    @Test
    public void executeTransactionCalledSuccessfullyOverOSGiInstance() {
        Object result = new Object();
        TransactionalTask mockTransactionalTask = mock(TransactionalTask.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.executeTransaction(mockTransactionalTask)).thenReturn(result);

        assertEquals(result, hazelcastOSGiInstance.executeTransaction(mockTransactionalTask));

        verify(mockHazelcastInstance).executeTransaction(mockTransactionalTask);
    }

    @Test
    public void executeTransactionWithOptionsCalledSuccessfullyOverOSGiInstance() {
        Object result = new Object();
        TransactionOptions transactionOptions = new TransactionOptions();
        TransactionalTask mockTransactionalTask = mock(TransactionalTask.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.executeTransaction(transactionOptions, mockTransactionalTask)).thenReturn(result);

        assertEquals(result, hazelcastOSGiInstance.executeTransaction(transactionOptions, mockTransactionalTask));

        verify(mockHazelcastInstance).executeTransaction(transactionOptions, mockTransactionalTask);
    }

    @Test
    public void newTransactionContextCalledSuccessfullyOverOSGiInstance() {
        TransactionContext mockTransactionContext = mock(TransactionContext.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.newTransactionContext()).thenReturn(mockTransactionContext);

        assertEquals(mockTransactionContext, hazelcastOSGiInstance.newTransactionContext());

        verify(mockHazelcastInstance).newTransactionContext();
    }

    @Test
    public void newTransactionContextWithOptionsCalledSuccessfullyOverOSGiInstance() {
        TransactionOptions transactionOptions = new TransactionOptions();
        TransactionContext mockTransactionContext = mock(TransactionContext.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.newTransactionContext(transactionOptions)).thenReturn(mockTransactionContext);

        assertEquals(mockTransactionContext, hazelcastOSGiInstance.newTransactionContext(transactionOptions));

        verify(mockHazelcastInstance).newTransactionContext(transactionOptions);
    }

    @Test
    public void getAtomicLongCalledSuccessfullyOverOSGiInstance() {
        IAtomicLong mockAtomicLong = mock(IAtomicLong.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        CPSubsystem cpSubsystem = mock(CPSubsystem.class);
        when(mockHazelcastInstance.getCPSubsystem()).thenReturn(cpSubsystem);
        when(mockHazelcastInstance.getCPSubsystem().getAtomicLong("my-atomiclong")).thenReturn(mockAtomicLong);

        assertEquals(mockAtomicLong, hazelcastOSGiInstance.getCPSubsystem().getAtomicLong("my-atomiclong"));

        verify(cpSubsystem).getAtomicLong("my-atomiclong");
    }

    @Test
    public void getAtomicReferenceCalledSuccessfullyOverOSGiInstance() {
        IAtomicReference<Object> mockAtomicReference = mock(IAtomicReference.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        CPSubsystem cpSubsystem = mock(CPSubsystem.class);
        when(mockHazelcastInstance.getCPSubsystem()).thenReturn(cpSubsystem);
        when(cpSubsystem.getAtomicReference("my-atomicreference")).thenReturn(mockAtomicReference);

        assertEquals(mockAtomicReference, hazelcastOSGiInstance.getCPSubsystem().getAtomicReference("my-atomicreference"));

        verify(cpSubsystem).getAtomicReference("my-atomicreference");
    }

    @Test
    public void getCountDownLatchCalledSuccessfullyOverOSGiInstance() {
        ICountDownLatch mockCountDownLatch = mock(ICountDownLatch.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        CPSubsystem cpSubsystem = mock(CPSubsystem.class);
        when(mockHazelcastInstance.getCPSubsystem()).thenReturn(cpSubsystem);
        when(cpSubsystem.getCountDownLatch("my-countdownlatch")).thenReturn(mockCountDownLatch);

        assertEquals(mockCountDownLatch, hazelcastOSGiInstance.getCPSubsystem().getCountDownLatch("my-countdownlatch"));

        verify(cpSubsystem).getCountDownLatch("my-countdownlatch");
    }

    @Test
    public void getSemaphoreCalledSuccessfullyOverOSGiInstance() {
        ISemaphore mockSemaphore = mock(ISemaphore.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        CPSubsystem cpSubsystem = mock(CPSubsystem.class);
        when(mockHazelcastInstance.getCPSubsystem()).thenReturn(cpSubsystem);
        when(cpSubsystem.getSemaphore("my-semaphore")).thenReturn(mockSemaphore);

        assertEquals(mockSemaphore, hazelcastOSGiInstance.getCPSubsystem().getSemaphore("my-semaphore"));

        verify(cpSubsystem).getSemaphore("my-semaphore");
    }

    @Test
    public void getDistributedObjectsCalledSuccessfullyOverOSGiInstance() {
        Collection<DistributedObject> mockDistributedObjects = mock(Collection.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getDistributedObjects()).thenReturn(mockDistributedObjects);

        assertEquals(mockDistributedObjects, hazelcastOSGiInstance.getDistributedObjects());

        verify(mockHazelcastInstance).getDistributedObjects();
    }

    @Test
    public void getDistributedObjectCalledSuccessfullyOverOSGiInstance() {
        DistributedObject mockDistributedObject = mock(DistributedObject.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance =
                createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getDistributedObject("my-service", "my-name")).thenReturn(mockDistributedObject);

        assertEquals(mockDistributedObject, hazelcastOSGiInstance.getDistributedObject("my-service", "my-name"));

        verify(mockHazelcastInstance).getDistributedObject("my-service", "my-name");
    }

    @Test
    public void addDistributedObjectListenerCalledSuccessfullyOverOSGiInstance() {
        DistributedObjectListener mockDistributedObjectListener = mock(DistributedObjectListener.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance =
                createHazelcastOSGiInstance(mockHazelcastInstance);

        UUID registrationId = UuidUtil.newUnsecureUUID();
        when(mockHazelcastInstance.addDistributedObjectListener(mockDistributedObjectListener)).thenReturn(registrationId);

        assertEquals(registrationId, hazelcastOSGiInstance.addDistributedObjectListener(mockDistributedObjectListener));

        verify(mockHazelcastInstance).addDistributedObjectListener(mockDistributedObjectListener);
    }

    @Test
    public void removeDistributedObjectListenerCalledSuccessfullyOverOSGiInstance() {
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        UUID registrationId = UuidUtil.newUnsecureUUID();

        when(mockHazelcastInstance.removeDistributedObjectListener(registrationId)).thenReturn(true);

        assertTrue(hazelcastOSGiInstance.removeDistributedObjectListener(registrationId));

        verify(mockHazelcastInstance).removeDistributedObjectListener(registrationId);
    }

    @Test
    public void getPartitionServiceCalledSuccessfullyOverOSGiInstance() {
        PartitionService mockPartitionService = mock(PartitionService.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getPartitionService()).thenReturn(mockPartitionService);

        assertEquals(mockPartitionService, hazelcastOSGiInstance.getPartitionService());

        verify(mockHazelcastInstance).getPartitionService();
    }

    @Test
    public void getSplitBrainProtectionServiceCalledSuccessfullyOverOSGiInstance() {
        SplitBrainProtectionService mockSplitBrainProtectionService = mock(SplitBrainProtectionService.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getSplitBrainProtectionService()).thenReturn(mockSplitBrainProtectionService);

        assertEquals(mockSplitBrainProtectionService, hazelcastOSGiInstance.getSplitBrainProtectionService());

        verify(mockHazelcastInstance).getSplitBrainProtectionService();
    }

    @Test
    public void getClientServiceCalledSuccessfullyOverOSGiInstance() {
        ClientService mockClientService = mock(ClientService.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getClientService()).thenReturn(mockClientService);

        assertEquals(mockClientService, hazelcastOSGiInstance.getClientService());

        verify(mockHazelcastInstance).getClientService();
    }

    @Test
    public void getLoggingServiceCalledSuccessfullyOverOSGiInstance() {
        LoggingService mockLoggingService = mock(LoggingService.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getLoggingService()).thenReturn(mockLoggingService);

        assertEquals(mockLoggingService, hazelcastOSGiInstance.getLoggingService());

        verify(mockHazelcastInstance).getLoggingService();
    }

    @Test
    public void getLifecycleServiceCalledSuccessfullyOverOSGiInstance() {
        LifecycleService mockLifecycleService = mock(LifecycleService.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getLifecycleService()).thenReturn(mockLifecycleService);

        assertEquals(mockLifecycleService, hazelcastOSGiInstance.getLifecycleService());

        verify(mockHazelcastInstance).getLifecycleService();
    }

    @Test
    public void getUserContextCalledSuccessfullyOverOSGiInstance() {
        ConcurrentMap<String, Object> mockUserContext = mock(ConcurrentMap.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getUserContext()).thenReturn(mockUserContext);

        assertEquals(mockUserContext, hazelcastOSGiInstance.getUserContext());

        verify(mockHazelcastInstance).getUserContext();
    }

    @Test
    public void getXAResourceCalledSuccessfullyOverOSGiInstance() {
        HazelcastXAResource mockXAResource = mock(HazelcastXAResource.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getXAResource()).thenReturn(mockXAResource);

        assertEquals(mockXAResource, hazelcastOSGiInstance.getXAResource());

        verify(mockHazelcastInstance).getXAResource();
    }

    @Test
    public void getConfigCalledSuccessfullyOverOSGiInstance() {
        Config mockConfig = mock(Config.class);
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        when(mockHazelcastInstance.getConfig()).thenReturn(mockConfig);

        assertEquals(mockConfig, hazelcastOSGiInstance.getConfig());

        verify(mockHazelcastInstance).getConfig();
    }

    @Test
    public void shutdownCalledSuccessfullyOverOSGiInstance() {
        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        HazelcastOSGiInstance hazelcastOSGiInstance = createHazelcastOSGiInstance(mockHazelcastInstance);

        hazelcastOSGiInstance.shutdown();

        verify(mockHazelcastInstance).shutdown();
    }
}
