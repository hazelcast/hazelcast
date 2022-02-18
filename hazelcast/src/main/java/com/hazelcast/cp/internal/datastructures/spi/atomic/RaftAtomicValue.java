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

package com.hazelcast.cp.internal.datastructures.spi.atomic;

import com.hazelcast.cp.CPGroupId;

/**
 * State-machine implementation of the Raft-based atomic value
 */
public abstract class RaftAtomicValue<T> {

    private final CPGroupId groupId;
    private final String name;

    public RaftAtomicValue(CPGroupId groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    public CPGroupId groupId() {
        return groupId;
    }

    public String name() {
        return name;
    }

    public abstract T get();
}
