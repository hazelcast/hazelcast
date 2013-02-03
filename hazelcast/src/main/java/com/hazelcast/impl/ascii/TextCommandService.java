/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl.ascii;

import com.hazelcast.impl.Node;
import com.hazelcast.impl.ascii.memcache.Stats;

public interface TextCommandService {

    boolean offer(String queueName, Object value);

    Object poll(String queueName, int seconds);

    Object poll(String queueName);

    void processRequest(TextCommand command);

    void sendResponse(TextCommand textCommand);

    Object get(String mapName, String key);

    byte[] getByteArray(String mapName, String key);

    Object put(String mapName, String key, Object value, int ttlSeconds);

    Object putIfAbsent(String mapName, String key, Object value, int ttlSeconds);

    Object replace(String mapName, String key, Object value);

    int getAdjustedTTLSeconds(int ttl);

    long incrementGetCount();

    long incrementSetCount();

    long incrementDeleteCount();

    long incrementHitCount();

    Object delete(String mapName, String key);

    Stats getStats();

    Node getNode();
}
