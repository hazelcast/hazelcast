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

package com.hazelcast.collection.impl.list;

import com.hazelcast.collection.impl.AbstractCollectionBackupTest;
import com.hazelcast.collection.impl.collection.AbstractCollectionProxyImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ListBackupTest extends AbstractCollectionBackupTest {

    private static final String LIST_NAME = "ListBackupTest";

    @Test
    public void testBackups() {
        config.getListConfig(LIST_NAME)
                .setBackupCount(BACKUP_COUNT)
                .setAsyncBackupCount(0);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        IList<Integer> list = hz.getList(LIST_NAME);
        for (int i = 0; i < ITEM_COUNT; i++) {
            list.add(i);
        }

        int partitionId = ((AbstractCollectionProxyImpl) list).getPartitionId();
        LOGGER.info("List " + LIST_NAME + " is stored in partition " + partitionId);

        testBackups(CollectionType.LIST, LIST_NAME, partitionId);
    }
}
