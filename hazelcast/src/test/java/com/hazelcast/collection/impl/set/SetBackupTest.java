/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.hazelcast.collection.impl.CollectionTestUtil.getBackupSet;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SetBackupTest extends AbstractCollectionBackupTest {

    @Test
    public void testBackupPromotion() {
        config.getSetConfig("default")
                .setBackupCount(1)
                .setAsyncBackupCount(0);

        testBackupPromotionInternal();
    }

    @Test
    public void testBackupMigration() {
        config.getSetConfig("default")
                .setBackupCount(BACKUP_COUNT)
                .setAsyncBackupCount(0);

        testBackupMigrationInternal();
    }

    @Override
    protected Collection<Integer> getHazelcastCollection(HazelcastInstance instance, String name) {
        return instance.getSet(name);
    }

    @Override
    protected Collection<Integer> getBackupCollection(HazelcastInstance instance, String name) {
        return getBackupSet(instance, name);
    }

    @Override
    protected int getPartitionId(Collection collection) {
        return ((AbstractCollectionProxyImpl) collection).getPartitionId();
    }
}
