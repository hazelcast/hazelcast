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

package com.hazelcast.replicatedmap.record;

import com.hazelcast.replicatedmap.ReplicatedMapService;

import java.util.Collection;
import java.util.Set;

public interface ReplicatedRecordStore {

    String getName();

    Object remove(Object key);

    Object get(Object key);

    Object put(Object key, Object value);

    boolean containsKey(Object key);

    boolean containsValue(Object value);

    ReplicatedRecord getReplicatedRecord(Object key);

    Set keySet();

    Collection values();

    Set entrySet();

    int size();

    void clear();

    boolean isEmpty();

    ReplicatedMapService getReplicatedMapService();

    void destroy();

}
