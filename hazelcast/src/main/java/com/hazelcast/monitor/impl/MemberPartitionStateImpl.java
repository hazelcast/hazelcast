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

package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.monitor.MemberPartitionState;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.util.JsonUtil.getArray;
import static com.hazelcast.util.JsonUtil.getBoolean;
import static com.hazelcast.util.JsonUtil.getInt;

/**
 * This class holds partition related statistics.
 */
public class MemberPartitionStateImpl implements MemberPartitionState {

    public static final int DEFAULT_PARTITION_COUNT = 271;

    List<Integer> partitions = new ArrayList<Integer>(DEFAULT_PARTITION_COUNT);
    boolean memberStateSafe;
    long migrationQueueSize;

    @Override
    public List<Integer> getPartitions() {
        return partitions;
    }

    @Override
    public boolean isMemberStateSafe() {
        return memberStateSafe;
    }

    public void setMemberStateSafe(boolean memberStateSafe) {
        this.memberStateSafe = memberStateSafe;
    }

    @Override
    public long getMigrationQueueSize() {
        return migrationQueueSize;
    }

    public void setMigrationQueueSize(long migrationQueueSize) {
        this.migrationQueueSize = migrationQueueSize;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        JsonArray partitionsArray = new JsonArray();
        for (Integer lsPartition : partitions) {
            partitionsArray.add(lsPartition);
        }
        root.add("partitions", partitionsArray);
        root.add("memberStateSafe", memberStateSafe);
        root.add("migrationQueueSize", migrationQueueSize);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        final JsonArray jsonPartitions = getArray(json, "partitions");
        for (JsonValue jsonPartition : jsonPartitions) {
            partitions.add(jsonPartition.asInt());
        }
        memberStateSafe = getBoolean(json, "memberStateSafe");
        migrationQueueSize = getInt(json, "migrationQueueSize");
    }

    @Override
    public String toString() {
        return "MemberPartitionStateImpl{"
                + "partitions=" + partitions
                + ", memberStateSafe=" + memberStateSafe
                + ", migrationQueueSize=" + migrationQueueSize
                + '}';
    }
}
