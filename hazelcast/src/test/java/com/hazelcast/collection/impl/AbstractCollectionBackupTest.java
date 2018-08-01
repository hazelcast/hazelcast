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

package com.hazelcast.collection.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractCollectionBackupTest extends HazelcastTestSupport {

    protected static final int BACKUP_COUNT = 4;

    private static final int ITEM_COUNT = 1000;

    private static final ILogger LOGGER = Logger.getLogger(AbstractCollectionBackupTest.class);

    protected TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
    protected Config config = new Config();

    /**
     * Returns an instance of a Hazelcast collection, e.g. {@link com.hazelcast.core.ISet}.
     *
     * @param instance the {@link HazelcastInstance} to retrieve the collection from
     * @param name     the name of the collection
     * @return an instance of a Hazelcast collection
     */
    protected abstract Collection<Integer> getHazelcastCollection(HazelcastInstance instance, String name);

    /**
     * Returns the backup values of a Hazelcast collection.
     *
     * @param instance the {@link HazelcastInstance} to retrieve the backup values from
     * @param name     the name of the collection
     * @return the backup values of a Hazelcast collection
     */
    protected abstract Collection<Integer> getBackupCollection(HazelcastInstance instance, String name);

    protected final void testBackupPromotionInternal() {
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        HazelcastInstance ownerInstance = instances[0];

        // we add items to the collection, so backups are created on promotedInstance
        String collectionName = generateKeyOwnedBy(ownerInstance);
        Collection<Integer> collection = getHazelcastCollection(ownerInstance, collectionName);
        for (int item = 0; item < ITEM_COUNT / 2; item++) {
            collection.add(item);
        }

        int partitionId = getPartitionIdViaReflection(collection);
        LOGGER.info("Collection " + collectionName + " is stored in partition " + partitionId);
        HazelcastInstance promotedInstance = getBackupInstance(instances, partitionId, 1);
        HazelcastInstance backupInstance = getBackupInstance(instances, partitionId, 2);

        // we terminate the ownerInstance, so the backups on promotedInstance have to be promoted
        factory.terminate(ownerInstance);
        waitAllForSafeState(factory.getAllHazelcastInstances());

        // we add additional items to the collection, so new backups have to be created on backupInstance
        collection = getHazelcastCollection(promotedInstance, collectionName);
        for (int item = ITEM_COUNT / 2; item < ITEM_COUNT; item++) {
            collection.add(item);
        }
        assertEqualsStringFormat("collection should contain %d items, but found %d", ITEM_COUNT, collection.size());

        // we check the backups on the backupInstance, which should contain the old and new items
        Collection<Integer> backupCollection = getBackupCollection(backupInstance, collectionName);
        assertEqualsStringFormat("backupCollection should contain %d items, but found %d",
                ITEM_COUNT, backupCollection.size());
        for (int item = 0; item < ITEM_COUNT; item++) {
            assertTrue("backupCollection should contain item " + item, backupCollection.contains(item));
        }
    }

    protected final void testBackupMigrationInternal() {
        HazelcastInstance ownerInstance = factory.newHazelcastInstance(config);

        // create items in the collection
        String collectionName = generateKeyOwnedBy(ownerInstance);
        Collection<Integer> collection = getHazelcastCollection(ownerInstance, collectionName);
        for (int item = 0; item < ITEM_COUNT; item++) {
            collection.add(item);
        }

        int partitionId = getPartitionIdViaReflection(collection);
        LOGGER.info("Collection " + collectionName + " is stored in partition " + partitionId);

        // scale up
        LOGGER.info("Scaling up to " + (BACKUP_COUNT + 1) + " members...");
        for (int backupCount = 1; backupCount <= BACKUP_COUNT; backupCount++) {
            factory.newHazelcastInstance(config);
            waitAllForSafeState(factory.getAllHazelcastInstances());
            assertEquals(backupCount + 1, factory.getAllHazelcastInstances().iterator().next().getCluster().getMembers().size());

            assertBackupCollection(collectionName, partitionId, backupCount);
        }

        // scale down
        LOGGER.info("Scaling down to 1 member...");
        for (int backupCount = BACKUP_COUNT - 1; backupCount > 0; backupCount--) {
            factory.getAllHazelcastInstances().iterator().next().shutdown();
            waitAllForSafeState(factory.getAllHazelcastInstances());
            assertEquals(backupCount + 1, factory.getAllHazelcastInstances().iterator().next().getCluster().getMembers().size());

            assertBackupCollection(collectionName, partitionId, backupCount);
        }
    }

    private void assertBackupCollection(String collectionName, int partitionId, int backupCount) {
        HazelcastInstance[] instances = factory.getAllHazelcastInstances().toArray(new HazelcastInstance[0]);
        LOGGER.info("Testing " + backupCount + " backups on " + instances.length + " members");

        for (int i = 1; i <= backupCount; i++) {
            HazelcastInstance backupInstance = getBackupInstance(instances, partitionId, i);
            Collection<Integer> backupCollection = getBackupCollection(backupInstance, collectionName);
            assertEqualsStringFormat("expected %d items in backupCollection, but found %d", ITEM_COUNT, backupCollection.size());
            for (int item = 0; item < ITEM_COUNT; item++) {
                assertTrue("backupCollection should contain item " + item, backupCollection.contains(item));
            }
        }
    }
}
