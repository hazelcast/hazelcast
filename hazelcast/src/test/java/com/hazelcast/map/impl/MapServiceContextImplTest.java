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

package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationservice.Operation.GENERIC_PARTITION_ID;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapServiceContextImplTest extends HazelcastTestSupport {

    private MapServiceContext mapServiceContext;

    @Before
    public void setUp() {
        HazelcastInstance instance = createHazelcastInstance();
        MapService service = getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
        mapServiceContext = service.getMapServiceContext();
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testGetPartitionContainer_withGenericPartitionId() {
        mapServiceContext.getPartitionContainer(GENERIC_PARTITION_ID);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testCreateRecordStore_withGenericPartitionId() {
        Config config = mapServiceContext.getNodeEngine().getConfig();
        MapContainer mapContainer = new MapContainer("anyName", config, mapServiceContext);

        mapServiceContext.createRecordStore(mapContainer, GENERIC_PARTITION_ID, null);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testGetRecordStore_withGenericPartitionId() {
        mapServiceContext.getRecordStore(GENERIC_PARTITION_ID, "anyMap");
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testGetRecordStoreWithSkipLoading_withGenericPartitionId() {
        mapServiceContext.getRecordStore(GENERIC_PARTITION_ID, "anyMap", true);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testGetExistingRecordStore_withGenericPartitionId() {
        mapServiceContext.getExistingRecordStore(GENERIC_PARTITION_ID, "anyMap");
    }
}
