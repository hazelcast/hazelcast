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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IFunction;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.OverridePropertyRule.set;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapExpirationBouncingMemberTest extends AbstractExpirationBouncingMemberTest {

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule
            = set(PROP_TASK_PERIOD_SECONDS, String.valueOf(ONE_SECOND));

    @Override
    protected Runnable[] getTasks() {
        IMap map = createMap();
        return new Runnable[]{new Get(map), new Set(map)};
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
            MapService service = nodeEngineImpl.getService(MapService.SERVICE_NAME);
            MapServiceContext mapServiceContext = service.getMapServiceContext();
            PartitionContainer[] containers = mapServiceContext.getPartitionContainers();
            InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();

            for (PartitionContainer container : containers) {
                boolean local = partitionService.getPartition(container.getPartitionId()).isLocal();
                Iterator<RecordStore> iterator = container.getMaps().values().iterator();
                while (iterator.hasNext()) {
                    RecordStore recordStore = iterator.next();
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

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.getMapConfig(name)
                .setMaxIdleSeconds(ONE_SECOND)
                .setBackupCount(backupCount);
        return config;
    }

    private IMap createMap() {
        HazelcastInstance testDriver = bounceMemberRule.getNextTestDriver();
        return testDriver.getMap(name);
    }

    private class Get implements Runnable {

        private final IMap map;

        Get(IMap map) {
            this.map = map;
        }

        @Override
        public void run() {
            for (int i = 0; i < keySpace; i++) {
                map.get(i);
            }
        }
    }

    private class Set implements Runnable {

        private final IMap map;

        Set(IMap map) {
            this.map = map;
        }

        @Override
        public void run() {
            for (int i = 0; i < keySpace; i++) {
                map.set(i, i);
            }
        }
    }
}
