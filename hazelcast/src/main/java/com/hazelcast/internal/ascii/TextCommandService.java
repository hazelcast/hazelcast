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

package com.hazelcast.internal.ascii;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.ascii.memcache.Stats;
import com.hazelcast.internal.ascii.rest.RestCallCollector;

import java.util.Map;
import java.util.Set;

@SuppressWarnings("checkstyle:methodcount")
public interface TextCommandService {

    boolean offer(String queueName, Object value);

    Object poll(String queueName, int seconds);

    Object poll(String queueName);

    void processRequest(TextCommand command);

    void sendResponse(TextCommand textCommand);

    Object get(String mapName, String key);

    Map<String, Object> getAll(String mapName, Set<String> keys);

    byte[] getByteArray(String mapName, String key);

    Object put(String mapName, String key, Object value);

    Object put(String mapName, String key, Object value, int ttlSeconds);

    Object putIfAbsent(String mapName, String key, Object value, int ttlSeconds);

    Object replace(String mapName, String key, Object value);

    void lock(String mapName, String key) throws InterruptedException;

    void unlock(String mapName, String key);

    int getAdjustedTTLSeconds(int ttl);

    long incrementDeleteHitCount(int inc);

    long incrementDeleteMissCount();

    long incrementGetHitCount();

    long incrementGetMissCount();

    long incrementSetCount();

    long incrementIncHitCount();

    long incrementIncMissCount();

    long incrementDecrHitCount();

    long incrementDecrMissCount();

    long incrementTouchCount();

    RestCallCollector getRestCallCollector();

    /**
     * Returns the size of the distributed queue instance with the specified name
     * @param queueName name of the distributed queue
     * @return the size of the distributed queue instance with the specified name
     */
    int size(String queueName);

    Object delete(String mapName, String key);

    void deleteAll(String mapName);

    Stats getStats();

    Node getNode();

    byte[] toByteArray(Object value);

    void stop();

    String getInstanceName();
}
