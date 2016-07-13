package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.operation.PutOperation;
import com.hazelcast.replicatedmap.impl.operation.ReplicateUpdateOperation;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapDataSerializerHook;
import com.hazelcast.replicatedmap.impl.operation.VersionResponsePair;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Random;

import static com.hazelcast.util.Preconditions.isNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ReplicatedMapReorderedReplicationTest extends HazelcastTestSupport {

    @Test
    public void testNonConvergingReplicatedMaps()
            throws Exception {
        final int nodeCount = 4;
        final int keyCount = 10000;
        final int threadCount = 2;

        updateFactory();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final Config config = new Config();
        config.setProperty("hazelcast.logging.type", "log4j");
        final HazelcastInstance[] instances = factory
                .newInstances(config, nodeCount);

        warmUpPartitions(instances);

        final int partitionId = randomPartitionOwnedBy(instances[0]).getPartitionId();

        final String mapName = randomMapName();
        final NodeEngineImpl nodeEngine = getNodeEngineImpl(instances[0]);

        final Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int startIndex = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = startIndex; j < keyCount; j += threadCount) {
                        put(nodeEngine, mapName, partitionId, j, j);
                    }
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        final ReplicatedRecordStore[] stores = new ReplicatedRecordStore[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            ReplicatedMapService service = getNodeEngineImpl(instances[i]).getService(ReplicatedMapService.SERVICE_NAME);
            service.triggerAntiEntropy();
            stores[i] = service.getReplicatedRecordStore(mapName, false, partitionId);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                long version = stores[0].getVersion();

                for (ReplicatedRecordStore store : stores) {
                    assertEquals(version, store.getVersion());
                    assertFalse(store.isStale(version));
                }
            }
        });

        for (int i = 0; i < keyCount; i++) {
            for (ReplicatedRecordStore store : stores) {
                assertTrue(store.containsKey(i));
            }
        }
    }

    private <K, V> V put(final NodeEngine nodeEngine, final String mapName, final int partitionId, K key, V value) {
        isNotNull(key, "key must not be null!");
        isNotNull(value, "value must not be null!");
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);

        PutOperation putOperation = new PutOperation(mapName, dataKey, dataValue);
        InternalCompletableFuture<Object> future = nodeEngine.getOperationService()
                                                             .invokeOnPartition(ReplicatedMapService.SERVICE_NAME, putOperation,
                                                                     partitionId);
        VersionResponsePair result = (VersionResponsePair) future.join();
        return nodeEngine.toObject(result.getResponse());
    }

    private void updateFactory() throws NoSuchFieldException, IllegalAccessException {
        Field field = ReplicatedMapDataSerializerHook.class.getDeclaredField("FACTORY");

        // remove final modifier from field
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.setAccessible(true);
        final DataSerializableFactory factory = (DataSerializableFactory) field.get(null);
        field.set(null, new TestReplicatedMapDataSerializerFactory(factory));

    }

    private static class TestReplicatedMapDataSerializerFactory implements DataSerializableFactory {

        private final DataSerializableFactory factory;

        public TestReplicatedMapDataSerializerFactory(DataSerializableFactory factory) {
            this.factory = factory;
        }

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            if (typeId == ReplicatedMapDataSerializerHook.OP_REPLICATE_UPDATE) {
                return new RetriedReplicateUpdateOperation();
            }

            return factory.create(typeId);
        }

    }

    private static class RetriedReplicateUpdateOperation extends ReplicateUpdateOperation {

        private static final Random random = new Random();

        @Override
        public void run() throws Exception {
            if (random.nextInt(10) < 2) {
                throw new RetryableHazelcastException();
            }

            super.run();
        }

    }

}
