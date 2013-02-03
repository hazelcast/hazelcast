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

package com.hazelcast.partition;

import com.hazelcast.core.Member;

import java.util.EventObject;

public class MigrationEvent extends EventObject {
    final int partitionId;
    final Member oldOwner;
    final Member newOwner;

    public MigrationEvent(Object source, int partitionId, Member oldOwner, Member newOwner) {
        super(source);
        this.partitionId = partitionId;
        this.oldOwner = oldOwner;
        this.newOwner = newOwner;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Member getOldOwner() {
        return oldOwner;
    }

    public Member getNewOwner() {
        return newOwner;
    }

    @Override
    public String toString() {
        return "MigrationEvent{" +
                "partitionId=" + partitionId +
                ", oldOwner=" + oldOwner +
                ", newOwner=" + newOwner +
                '}';
    }
}
