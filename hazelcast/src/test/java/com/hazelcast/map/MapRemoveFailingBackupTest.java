/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.map.impl.operation.BaseRemoveOperation;
import com.hazelcast.map.impl.operation.KeyBasedMapOperation;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ThreadUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MapRemoveFailingBackupTest extends HazelcastTestSupport {

    @Test
    public void testMapRemoveFailingBackupShouldNotLeadToStaleDataWhenReadBackupIsEnabled() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final String mapName = randomMapName();
        final String key = "2";
        final String value = "value2";
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_BACKUP_SYNC_INTERVAL, "5");
        config.getMapConfig(mapName).setReadBackupData(true);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        final HazelcastInstanceImpl hz1Impl = TestUtil.getHazelcastInstanceImpl(hz1);
        final IMap<Object, Object> map1 = hz1.getMap(mapName);
        final IMap<Object, Object> map2 = hz2.getMap(mapName);
        MapProxyImpl<Object, Object> mock1 = (MapProxyImpl<Object, Object>) spy(map1);
        when(mock1.remove(anyString())).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                NodeEngineImpl nodeEngine = hz1Impl.node.nodeEngine;
                Object object = invocation.getArguments()[0];
                final Data key = nodeEngine.toData(object);
                RemoveOperation operation = new RemoveOperation(mapName, key);
                int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
                operation.setThreadId(ThreadUtil.getThreadId());
                OperationService operationService = nodeEngine.getOperationService();
                InternalCompletableFuture<Data> f = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .setResultDeserialized(false).invoke();
                Data result = f.get();
                return nodeEngine.toObject(result);
            }
        });

        mock1.put(key, value);
        mock1.remove(key);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(map1.get(key));
                assertNull(map2.get(key));
            }
        }, 30);
    }


    private static class RemoveOperation extends BaseRemoveOperation {

        boolean successful;

        public RemoveOperation(String name, Data dataKey) {
            super(name, dataKey);
        }

        public RemoveOperation() {
        }

        public void run() {
            dataOldValue = mapService.getMapServiceContext().toData(recordStore.remove(dataKey));
            successful = dataOldValue != null;
        }

        public void afterRun() {
            if (successful) {
                super.afterRun();
            }
        }

        @Override
        public Operation getBackupOperation() {
            return new ExceptionThrowingRemoveBackupOperation(name, dataKey);
        }

        public boolean shouldBackup() {
            return successful;
        }
    }

    private static class ExceptionThrowingRemoveBackupOperation extends KeyBasedMapOperation {
        private ExceptionThrowingRemoveBackupOperation() {
        }

        public ExceptionThrowingRemoveBackupOperation(String name, Data dataKey) {
            super(name, dataKey);
        }

        @Override
        public void run() throws Exception {
            throw new UnsupportedOperationException("Don't panic this is what we want!");
        }

        @Override
        public Object getResponse() {
            return Boolean.TRUE;
        }
    }
}
