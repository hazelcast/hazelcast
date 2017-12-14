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

package com.hazelcast.collection.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.util.Collection;

import static com.hazelcast.collection.impl.CollectionTestUtil.getBackupList;
import static com.hazelcast.collection.impl.CollectionTestUtil.getBackupQueue;
import static com.hazelcast.collection.impl.CollectionTestUtil.getBackupSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractCollectionBackupTest extends HazelcastTestSupport {

    protected enum CollectionType {
        LIST,
        QUEUE,
        SET
    }

    protected static final int ITEM_COUNT = 1000;
    protected static final int BACKUP_COUNT = 4;

    protected static final ILogger LOGGER = Logger.getLogger(AbstractCollectionBackupTest.class);

    protected TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
    protected Config config = new Config();

    protected final void testBackups(CollectionType collectionType, String collectionName, int partitionId) {
        // scale up
        LOGGER.info("Scaling up to " + (BACKUP_COUNT + 1) + " members...");
        for (int backupCount = 1; backupCount <= BACKUP_COUNT; backupCount++) {
            factory.newHazelcastInstance(config);
            waitAllForSafeState(factory.getAllHazelcastInstances());
            assertEquals(backupCount + 1, factory.getAllHazelcastInstances().iterator().next().getCluster().getMembers().size());

            assertBackupCollection(collectionType, collectionName, partitionId, backupCount);
        }

        // scale down
        LOGGER.info("Scaling down to 1 member...");
        for (int backupCount = BACKUP_COUNT - 1; backupCount > 0; backupCount--) {
            factory.getAllHazelcastInstances().iterator().next().shutdown();
            waitAllForSafeState(factory.getAllHazelcastInstances());
            assertEquals(backupCount + 1, factory.getAllHazelcastInstances().iterator().next().getCluster().getMembers().size());

            assertBackupCollection(collectionType, collectionName, partitionId, backupCount);
        }
    }

    private void assertBackupCollection(CollectionType collectionType, String collectionName, int partitionId, int backupCount) {
        HazelcastInstance[] instances = factory.getAllHazelcastInstances().toArray(new HazelcastInstance[0]);
        LOGGER.info("Testing " + backupCount + " backups on " + instances.length + " members");

        for (int i = 1; i <= backupCount; i++) {
            HazelcastInstance backupInstance = getBackupInstance(instances, partitionId, i);
            Collection<Integer> backupCollection = null;
            switch (collectionType) {
                case LIST:
                    backupCollection = getBackupList(backupInstance, collectionName);
                    break;
                case QUEUE:
                    backupCollection = getBackupQueue(backupInstance, collectionName);
                    break;
                case SET:
                    backupCollection = getBackupSet(backupInstance, collectionName);
                    break;
                default:
                    fail("Unknown collectionType " + collectionType);
            }

            assertEqualsStringFormat("expected %d items in backupCollection, but found %d", ITEM_COUNT, backupCollection.size());
            for (int item = 0; item < ITEM_COUNT; item++) {
                assertTrue("backupCollection should contain item " + item, backupCollection.contains(item));
            }
        }
    }
}
