package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.PutOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OperationServiceImpl_asyncInvokeOnPartitionTest extends HazelcastTestSupport {

    @Test
    public void testAsyncInvocation_SamePartitionThread_DifferentPartition() {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        String key = randomString();
        IMap<String, String> map = instance.getMap(name);
        map.put(key, name);
        map.executeOnKey(key, new InvocationEntryProcessor());
        assertOpenEventually(InvocationEntryProcessor.latch, 10);
    }

    public static class InvocationEntryProcessor implements EntryProcessor, HazelcastInstanceAware {

        public static final CountDownLatch latch = new CountDownLatch(1);

        transient HazelcastInstance instance;

        @Override
        public Object process(Map.Entry entry) {
            Node node = getNode(instance);
            NodeEngineImpl nodeEngine = node.nodeEngine;
            OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
            Data sourceKey = nodeEngine.toData(entry.getKey());
            Data key = generateKey_FallsToSamePartitionThread_ButDifferentPartition(nodeEngine, sourceKey);
            Data val = nodeEngine.toData(randomString());
            PutOperation op = new PutOperation((String) entry.getValue(), key, val, -1);
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            operationService.asyncInvokeOnPartition(MapService.SERVICE_NAME, op, partitionId,
                    new ExecutionCallback<Object>() {
                        @Override
                        public void onResponse(Object response) {
                            latch.countDown();
                        }

                        @Override
                        public void onFailure(Throwable t) {

                        }
                    });
            return null;
        }

        private Data generateKey_FallsToSamePartitionThread_ButDifferentPartition(NodeEngineImpl nodeEngine, Data sourceKey) {
            InternalPartitionService partitionService = nodeEngine.getPartitionService();
            int sourcePartitionId = partitionService.getPartitionId(sourceKey);
            OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
            int threadCount = operationService.operationExecutor.getPartitionOperationThreadCount();
            int sourceThreadId = sourcePartitionId % threadCount;

            while (true) {
                Data key = nodeEngine.toData(randomString());
                int partitionId = partitionService.getPartitionId(key);
                int threadId = partitionId % threadCount;
                if (sourcePartitionId != partitionId && sourceThreadId == threadId) {
                    return key;
                }
            }
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            instance = hazelcastInstance;
        }
    }
}
