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

package com.hazelcast.collection.impl.set;

import com.hazelcast.collection.impl.AbstractCollectionBackupTest;
import com.hazelcast.collection.impl.collection.AbstractCollectionProxyImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SetBackupTest extends AbstractCollectionBackupTest {

    private static final String SET_NAME = "SetBackupTest";

    @Test
    public void testBackups() {
        config.getSetConfig(SET_NAME)
                .setBackupCount(BACKUP_COUNT)
                .setAsyncBackupCount(0);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        ISet<Integer> set = hz.getSet(SET_NAME);
        for (int i = 0; i < ITEM_COUNT; i++) {
            set.add(i);
        }

        int partitionId = ((AbstractCollectionProxyImpl) set).getPartitionId();
        LOGGER.info("Set " + SET_NAME + " is stored in partition " + partitionId);

        testBackups(CollectionType.SET, SET_NAME, partitionId);
    }
}
