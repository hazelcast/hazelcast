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

package com.hazelcast.internal.eviction;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IFunction;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.spi.CachingProvider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.OverridePropertyRule.set;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CacheExpirationBouncingMemberTest extends AbstractExpirationBouncingMemberTest {

    private static final long EXPIRY_MILLIS = TimeUnit.SECONDS.toMillis(ONE_SECOND);
    private static final HazelcastExpiryPolicy EXPIRY_POLICY
            = new HazelcastExpiryPolicy(EXPIRY_MILLIS, EXPIRY_MILLIS, EXPIRY_MILLIS);

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule
            = set(PROP_TASK_PERIOD_SECONDS, String.valueOf(ONE_SECOND));

    @Override
    protected Runnable[] getTasks() {
        Cache cache = createCache();
        return new Runnable[]{new Get(cache), new Set(cache)};
    }

    @Override
    protected IFunction<HazelcastInstance, List> newExceptionMsgCreator() {
        return new ExceptionMsgCreator();
    }

    public static class ExceptionMsgCreator implements IFunction<HazelcastInstance, List> {
        @Override
        public List apply(HazelcastInstance instance) {
            List unexpiredMsg = new ArrayList();

            NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
            CacheService service = nodeEngineImpl.getService(CacheService.SERVICE_NAME);
            InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
            for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
                CachePartitionSegment container = service.getSegment(partitionId);
                boolean local = partitionService.getPartition(partitionId).isLocal();
                Iterator<ICacheRecordStore> iterator = container.recordStoreIterator();
                while (iterator.hasNext()) {
                    ICacheRecordStore recordStore = iterator.next();
                    boolean expirable = recordStore.isExpirable();

                    if (recordStore.size() > 0 || recordStore.getExpiredKeysQueue().size() > 0) {
                        unexpiredMsg.add(recordStore.getPartitionId());
                        unexpiredMsg.add(recordStore.size());
                        unexpiredMsg.add(recordStore.getExpiredKeysQueue().size());
                        unexpiredMsg.add(expirable);
                        unexpiredMsg.add(local);
                        unexpiredMsg.add(nodeEngineImpl.getClusterService().getLocalMember().getAddress());
                    }
                }
            }
            return unexpiredMsg;
        }
    }

    private CacheConfig getCacheConfig() {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(name);
        cacheConfig.setBackupCount(backupCount);
        cacheConfig.setExpiryPolicyFactory(FactoryBuilder.factoryOf(EXPIRY_POLICY));
        return cacheConfig;
    }

    private Cache createCache() {
        HazelcastInstance testDriver = bounceMemberRule.getNextTestDriver();
        CachingProvider provider = createServerCachingProvider(testDriver);
        return provider.getCacheManager().createCache(name, getCacheConfig());
    }

    private class Get implements Runnable {

        private final Cache<Integer, Integer> cache;

        Get(Cache cache) {
            this.cache = cache;
        }

        @Override
        public void run() {
            for (int i = 0; i < keySpace; i++) {
                cache.get(i);
            }
        }
    }

    private class Set implements Runnable {

        private final Cache<Integer, Integer> cache;

        Set(Cache cache) {
            this.cache = cache;
        }

        @Override
        public void run() {
            for (int i = 0; i < keySpace; i++) {
                cache.put(i, i);
            }
        }
    }
}
