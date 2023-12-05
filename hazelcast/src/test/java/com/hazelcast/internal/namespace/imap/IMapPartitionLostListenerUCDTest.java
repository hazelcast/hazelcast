/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.namespace.imap;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionEventManager;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.spi.impl.NodeEngineImpl;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;

public class IMapPartitionLostListenerUCDTest extends IMapUCDTest {
    @Override
    public void test() throws Exception {
        MapPartitionLostListener classInstance = getClassInstance();
        map.addPartitionLostListener(classInstance);

        // Fire a fake partition lost event
        NodeEngineImpl nodeEngine = getNodeEngineImpl(member);
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        PartitionEventManager eventManager = partitionService.getPartitionEventManager();
        eventManager.sendPartitionLostEvent(1, InternalPartition.MAX_BACKUP_COUNT);

        assertListenerFired("partitionLost");
    }

    @Override
    protected String getUserDefinedClassName() {
        return "usercodedeployment.CustomMapPartitionLostListener";
    }
}
