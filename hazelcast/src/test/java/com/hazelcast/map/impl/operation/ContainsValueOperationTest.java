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

package com.hazelcast.map.impl.operation;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ContainsValueOperationTest extends HazelcastTestSupport {

    private final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
    private HazelcastInstance member1;

    @Before
    public void setUp() {
        Config config = getConfig();
        member1 = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
    }

    @Test
    public void test_ContainValueOperation() throws ExecutionException, InterruptedException {
        int value = 1;
        String mapName = randomMapName();

        IMap<String, Integer> map = member1.getMap(mapName);
        String key = generateKeyNotOwnedBy(member1);
        map.put(key, value);

        Future future = executeOperation(map, key, value);

        assertTrue((Boolean) future.get());
    }

    private InternalCompletableFuture<Object> executeOperation(Map map, String key, int value) {
        int partitionId = getNode(member1).getPartitionService().getPartitionId(key);
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapServiceContext mapServiceContext = ((MapService) mapProxy.getService()).getMapServiceContext();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapProxy.getName());
        OperationFactory operationFactory
                = operationProvider.createContainsValueOperationFactory(mapProxy.getName(), mapServiceContext.toData(value));
        Operation operation = operationFactory.createOperation();

        OperationServiceImpl operationService = getOperationService(member1);
        return operationService.createInvocationBuilder(MapService.SERVICE_NAME, operation, partitionId).invoke();
    }
}
