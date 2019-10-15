/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalMapStatsProviderTest extends HazelcastTestSupport {

    //https://github.com/hazelcast/hazelcast/issues/11598
    @Test
    public void testRedundantPartitionMigrationWhenManCenterConfigured() {
        Config config = new Config();
        config.getManagementCenterConfig().setEnabled(true);
        config.getManagementCenterConfig().setUrl(format("http://localhost:%d%s/", 8085, "/mancenter"));

        //don't need start management center, just configure it
        final HazelcastInstance instance = createHazelcastInstance(config);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                //check partition migration triggered or not
                int partitionStateVersion = getNode(instance).getPartitionService().getPartitionStateVersion();
                assertEquals(0, partitionStateVersion);
            }
        }, 5);
    }
}
