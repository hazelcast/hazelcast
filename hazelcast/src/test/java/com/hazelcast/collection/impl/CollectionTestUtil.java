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

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.collection.impl.collection.CollectionService;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueItem;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISet;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;

public final class CollectionTestUtil {

    private CollectionTestUtil() {
    }

    /**
     * Returns all backup items of an {@link IList} by a given list name.
     * <p>
     * Note: You have to provide the {@link HazelcastInstance} you want to retrieve the backups from.
     * Use {@link HazelcastTestSupport#getBackupInstance} to retrieve the backup instance for a given replica index.
     *
     * @param backupInstance the {@link HazelcastInstance} to retrieve the backups from
     * @param listName       the list name
     * @return a {@link List} with the backup items
     */
    public static <E> List<E> getBackupList(HazelcastInstance backupInstance, String listName) {
        CollectionService service = getNodeEngineImpl(backupInstance).getService(ListService.SERVICE_NAME);
        CollectionContainer collectionContainer = service.getContainerMap().get(listName);
        // the replica items are retrieved via `getMap()`, the primary items via `getCollection()`
        Map<Long, CollectionItem> map = collectionContainer.getMap();

        List<E> backupList = new ArrayList<E>(map.size());
        SerializationService serializationService = getNodeEngineImpl(backupInstance).getSerializationService();
        for (CollectionItem collectionItem : map.values()) {
            E value = serializationService.toObject(collectionItem.getValue());
            backupList.add(value);
        }
        return backupList;
    }

    /**
     * Returns all backup items of an {@link IQueue} by a given queue name.
     * <p>
     * Note: You have to provide the {@link HazelcastInstance} you want to retrieve the backups from.
     * Use {@link HazelcastTestSupport#getBackupInstance} to retrieve the backup instance for a given replica index.
     *
     * @param backupInstance the {@link HazelcastInstance} to retrieve the backups from
     * @param queueName      the queue name
     * @return a {@link List} with the backup items
     */
    public static <E> Queue<E> getBackupQueue(HazelcastInstance backupInstance, String queueName) {
        QueueService service = getNodeEngineImpl(backupInstance).getService(QueueService.SERVICE_NAME);
        QueueContainer container = service.getOrCreateContainer(queueName, true);
        Map<Long, QueueItem> map = container.getBackupMap();

        Queue<E> backupQueue = new LinkedList<E>();
        SerializationService serializationService = getNodeEngineImpl(backupInstance).getSerializationService();
        for (QueueItem queueItem : map.values()) {
            E value = serializationService.toObject(queueItem.getData());
            backupQueue.add(value);
        }
        return backupQueue;
    }

    /**
     * Returns all backup items of an {@link ISet} by a given queue name.
     * <p>
     * Note: You have to provide the {@link HazelcastInstance} you want to retrieve the backups from.
     * Use {@link HazelcastTestSupport#getBackupInstance} to retrieve the backup instance for a given replica index.
     *
     * @param backupInstance the {@link HazelcastInstance} to retrieve the backups from
     * @param setName        the set name
     * @return a {@link List} with the backup items
     */
    public static <E> Set<E> getBackupSet(HazelcastInstance backupInstance, String setName) {
        CollectionService service = getNodeEngineImpl(backupInstance).getService(SetService.SERVICE_NAME);
        CollectionContainer collectionContainer = service.getContainerMap().get(setName);
        // the replica items are retrieved via `getMap()`, the primary items via `getCollection()`
        Map<Long, CollectionItem> map = collectionContainer.getMap();

        Set<E> backupSet = new HashSet<E>();
        SerializationService serializationService = getNodeEngineImpl(backupInstance).getSerializationService();
        for (CollectionItem collectionItem : map.values()) {
            E value = serializationService.toObject(collectionItem.getValue());
            backupSet.add(value);
        }
        return backupSet;
    }
}
