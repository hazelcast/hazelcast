/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapReadBackupDataAsyncTest extends HazelcastTestSupport {

    @Test
    public void testGetAsync_ReadsFromLocalBackup() throws Exception {
        String mapName = randomMapName();
        Config config = getConfig();

        config.getMapConfig(mapName)
                .setReadBackupData(true)
                .setBackupCount(1);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        // Warm up partitions to ensure ownership is established
        assertClusterSizeEventually(2, hz1, hz2);
        warmUpPartitions(hz1, hz2);

        IMap<String, String> map1 = hz1.getMap(mapName);
        IMap<String, String> map2 = hz2.getMap(mapName);

        String key = generateKeyOwnedBy(hz1);
        String value = "async-backup-value";

        map1.put(key, value);

        //Wait for the backup to be replicated to hz2.
        assertTrueEventually(() -> {
            long backupEntryCount = hz2.getMap(mapName).getLocalMapStats().getBackupEntryCount();
            assertEquals("Backup should be present on node 2", 1, backupEntryCount);
        });

        CompletionStage<String> stage = map2.getAsync(key);

        String result = stage.toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertEquals("Value retrieved via getAsync should match the backup data", value, result);
        // Optional: Verify that the owner (hz1) didn't receive a GetOperation
        // This proves the read happened locally on hz2
        long ownerGetCount = hz1.getMap(mapName).getLocalMapStats().getGetOperationCount();
        assertEquals("Owner should not have processed a GetOperation", 0, ownerGetCount);
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        // Reduce partition count for faster startup in tests
        config.setProperty("hazelcast.partition.count", "10");
        return config;
    }
}
