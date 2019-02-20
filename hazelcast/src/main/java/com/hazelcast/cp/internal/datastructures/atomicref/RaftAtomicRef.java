/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.cp.CPGroupId;

/**
 * State-machine implementation of the Raft-based atomic reference
 */
public class RaftAtomicRef {

    private final CPGroupId groupId;
    private final String name;
    private Data value;

    RaftAtomicRef(CPGroupId groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    RaftAtomicRef(CPGroupId groupId, String name, Data value) {
        this.groupId = groupId;
        this.name = name;
        this.value = value;
    }

    public CPGroupId groupId() {
        return groupId;
    }

    public String name() {
        return name;
    }

    public Data get() {
        return value;
    }

    public void set(Data value) {
        this.value = value;
    }

    public boolean contains(Data expected) {
        return (value != null) ? value.equals(expected) : (expected == null);
    }
}
