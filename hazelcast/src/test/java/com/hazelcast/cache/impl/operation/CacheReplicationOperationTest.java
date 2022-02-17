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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheReplicationOperationTest extends HazelcastTestSupport {

    @Test
    public void sendsConfigObjectOverWire() throws Exception {
        // new config
        CacheConfig config = new CacheConfig("test-cache");

        // add config to cache service
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(createHazelcastInstance());
        CacheService cacheService = nodeEngineImpl.getService(CacheService.SERVICE_NAME);
        cacheService.putCacheConfigIfAbsent(config);

        CachePartitionSegment segment = new CachePartitionSegment(cacheService, 0);
        segment.getOrCreateRecordStore(config.getNameWithPrefix());
        Collection<ServiceNamespace> namespaces = segment.getAllNamespaces(0);
        assertEquals(1, namespaces.size());

        // create operation
        CacheReplicationOperation operation = new CacheReplicationOperation();
        operation.prepare(segment, namespaces, 0);

        // serialize & deserialize operation
        Data data = nodeEngineImpl.toData(operation);
        CacheReplicationOperation cacheReplicationOperation = (CacheReplicationOperation) nodeEngineImpl.toObject(data);

        // new operation instance should have previously added config.
        assertContains(cacheReplicationOperation.getConfigs(), config);
    }
}
