package com.hazelcast.cache;

import com.hazelcast.cache.impl.CacheEventListener;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.client.CacheSingleInvalidationMessage;
import com.hazelcast.cache.impl.operation.CacheDestroyOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheDestroyTest extends CacheTestSupport {
    private static final int INSTANCE_COUNT = 2;

    private TestHazelcastInstanceFactory factory = getInstanceFactory(INSTANCE_COUNT);
    private HazelcastInstance[] hazelcastInstances;
    private HazelcastInstance hazelcastInstance;

    protected TestHazelcastInstanceFactory getInstanceFactory(int instanceCount) {
        return createHazelcastInstanceFactory(instanceCount);
    }

    @Override
    protected void onSetup() {
        Config config = createConfig();
        hazelcastInstances = new HazelcastInstance[INSTANCE_COUNT];
        for (int i = 0; i < INSTANCE_COUNT; i++) {
            hazelcastInstances[i] = factory.newHazelcastInstance(config);
        }
        warmUpPartitions(hazelcastInstances);
        waitAllForSafeState(hazelcastInstances);
        hazelcastInstance = hazelcastInstances[0];
    }

    @Override
    protected void onTearDown() {
        factory.shutdownAll();
        hazelcastInstances = null;
        hazelcastInstance = null;
    }


    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig cacheConfig = super.createCacheConfig();
        cacheConfig.setBackupCount(INSTANCE_COUNT - 1);
        return cacheConfig;
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    @Test
    public void test_cacheDestroyOperation() throws ExecutionException, InterruptedException {
        final ICache<String, String> cache = createCache();

        NodeEngineImpl nodeEngine1 = getNode(getHazelcastInstance()).getNodeEngine();
        final ICacheService cacheService1 = nodeEngine1.getService(ICacheService.SERVICE_NAME);
        InternalOperationService operationService1 = nodeEngine1.getOperationService();

        NodeEngineImpl nodeEngine2 = getNode(hazelcastInstances[1]).getNodeEngine();
        final ICacheService cacheService2 = nodeEngine2.getService(ICacheService.SERVICE_NAME);

        final CacheConfig config = cache.getConfiguration(CacheConfig.class);
        final String nameWithPrefix = config.getNameWithPrefix();
        assertNotNull(cacheService1.getCacheConfig(nameWithPrefix));
        assertNotNull(cacheService2.getCacheConfig(nameWithPrefix));

        // Invoke on single node and the operation is also forward to others nodes by the operation itself
        operationService1.invokeOnTarget(ICacheService.SERVICE_NAME,
                                         new CacheDestroyOperation(nameWithPrefix),
                                         nodeEngine1.getThisAddress());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(cacheService1.getCacheConfig(nameWithPrefix));
                assertNull(cacheService2.getCacheConfig(nameWithPrefix));
            }
        });
    }

    @Test
    public void testInvalidationListenerCallCount() {
        final ICache<String, String> cache = createCache();

        final AtomicInteger counter = new AtomicInteger(0);

        final CacheConfig config = cache.getConfiguration(CacheConfig.class);

        registerInvalidationListener(new CacheEventListener() {
            @Override
            public void handleEvent(Object eventObject) {
                if (eventObject instanceof CacheSingleInvalidationMessage) {
                    CacheSingleInvalidationMessage event = (CacheSingleInvalidationMessage) eventObject;
                    if (null == event.getKey() && config.getNameWithPrefix().equals(event.getName())) {
                        counter.incrementAndGet();
                    }
                }
            }
        }, config.getNameWithPrefix());

        cache.destroy();

        // Make sure that one event is received
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(1, counter.get());
            }
        }, 2);

        // Make sure that the callback is not called for a while
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(counter.get() <= 1);
            }
        }, 3);

    }

    private void registerInvalidationListener(CacheEventListener cacheEventListener, String name) {
        HazelcastInstanceProxy hzInstance = (HazelcastInstanceProxy) this.hazelcastInstance;
        hzInstance.getOriginal().node.getNodeEngine().getEventService()
                                     .registerListener(ICacheService.SERVICE_NAME, name, cacheEventListener);
    }

}
