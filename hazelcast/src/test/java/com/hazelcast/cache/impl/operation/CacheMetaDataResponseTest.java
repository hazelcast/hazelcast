/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheEventHandler;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheGetInvalidationMetaDataOperation.MetaDataResponse;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheMetaDataResponseTest extends HazelcastTestSupport {

    private final String cacheName1 = "1cache";
    private final String cacheName2 = "2cache";
    private final String cacheName3 = "3cache";

    private int partitionSequenceCount;

    @Before
    public void setUp() throws Exception {
        partitionSequenceCount = RandomPicker.getInt(10);
    }

    @Test
    public void name() throws Exception {
        HazelcastInstance hz = createHazelcastInstance();
        warmUpPartitions(hz);
        InternalOperationService operationService = getOperationService(hz);
        InternalPartitionService partitionService = getPartitionService(hz);
        int partitionCount = partitionService.getPartitionCount();

        List<String> cacheNames = new ArrayList<String>();
        cacheNames.add(cacheName1);
        cacheNames.add(cacheName2);
        cacheNames.add(cacheName3);

        generateUuidAndSequences(hz, partitionCount, cacheNames);

        MetaDataResponse metaDataResponse = getMetaDataResponse(hz, operationService, cacheNames);

        List<Object> namePartitionSequenceList = metaDataResponse.getNamePartitionSequenceList();
        for (int i = 0; i < namePartitionSequenceList.size(); ) {
            Object item = namePartitionSequenceList.get(i++);

            if (item instanceof String) {
                assertInstanceOf(String.class, item);
            } else {
                assertInstanceOf(Integer.class, item);
                assertInstanceOf(Long.class, namePartitionSequenceList.get(i++));
            }
        }

    }

    protected MetaDataResponse getMetaDataResponse(HazelcastInstance hz, InternalOperationService operationService, List<String> cacheNames) throws InterruptedException, java.util.concurrent.ExecutionException {
        CacheGetInvalidationMetaDataOperation operation = new CacheGetInvalidationMetaDataOperation(cacheNames);
        Address address = hz.getCluster().getLocalMember().getAddress();
        InternalCompletableFuture<Object> future = operationService.invokeOnTarget(SERVICE_NAME, operation, address);

        return (MetaDataResponse) future.get();
    }

    protected void generateUuidAndSequences(HazelcastInstance hz, int partitionCount, List<String> cacheNames) {
        CacheService cacheService = getNodeEngineImpl(hz).getService(SERVICE_NAME);
        CacheEventHandler cacheEventHandler = cacheService.getCacheEventHandler();
        MetaDataGenerator metaDataGenerator = cacheEventHandler.getMetaDataGenerator();

        for (String cacheName : cacheNames) {
            for (int partition = 0; partition < partitionCount; partition++) {
                for (int j = 0; j < partitionSequenceCount; j++) {
                    metaDataGenerator.nextSequence(cacheName, partition);
                }

                metaDataGenerator.getOrCreateUuid(partition);
            }
        }
    }
}