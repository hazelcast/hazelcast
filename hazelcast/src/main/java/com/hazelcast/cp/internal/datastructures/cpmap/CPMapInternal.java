/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.cp.CPGroupId;

import java.util.HashMap;
import java.util.Map;

/**
 * State-machine implementation of the Raft-based atomic long
 */
public class CPMapInternal {

    private Map<Object, Object> map;

    CPMapInternal(CPGroupId groupId, String name) {
        this(groupId, name, new HashMap<>());
    }

    CPMapInternal(CPGroupId groupId, String name, Map<Object, Object> map) {
        this.groupId = groupId;
        this.name = name;
        this.map = map;
    }

    private final CPGroupId groupId;
    private final String name;

    public CPGroupId groupId() {
        return groupId;
    }

    public String name() {
        return name;
    }

    public void set(Object key, Object value) {
        map.put(key, value);
    }

    public Object get(Object key) {
        return map.get(key);
    }

    @Override
    public String toString() {
        return "CPMapInternal{" + "groupId=" + groupId() + ", name='" + name() + '\'' + ", map=" + map + '}';
    }
}
