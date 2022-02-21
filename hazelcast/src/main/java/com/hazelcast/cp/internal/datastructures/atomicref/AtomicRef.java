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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.cp.internal.datastructures.spi.atomic.RaftAtomicValue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.cp.CPGroupId;

import java.util.Objects;

/**
 * State-machine implementation of the Raft-based atomic reference
 */
public class AtomicRef extends RaftAtomicValue<Data> {

    private Data value;

    AtomicRef(CPGroupId groupId, String name, Data value) {
        super(groupId, name);
        this.value = value;
    }

    @Override
    public Data get() {
        return value;
    }

    public void set(Data value) {
        this.value = value;
    }

    public boolean contains(Data expected) {
        return Objects.equals(value, expected);
    }

    @Override
    public String toString() {
        return "AtomicRef{" + "groupId=" + groupId() + ", name='" + name() + '\'' + ", value=" + value + '}';
    }
}
