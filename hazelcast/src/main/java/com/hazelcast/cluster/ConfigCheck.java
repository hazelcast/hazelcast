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

package com.hazelcast.cluster;

import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import java.io.IOException;

import static com.hazelcast.util.EmptyStatement.ignore;

/**
 * Checks whether vital node configurations are the same or not.
 */
public final class ConfigCheck implements IdentifiedDataSerializable {

    private String groupName;

    private String groupPassword;

    private String joinerType;

    private boolean partitionGroupEnabled;

    private PartitionGroupConfig.MemberGroupType memberGroupType;

    public ConfigCheck() {
    }

    public boolean isCompatible(ConfigCheck other) {
        if (!groupName.equals(other.groupName)) {
            return false;
        } else if (!groupPassword.equals(other.groupPassword)) {
            throw new HazelcastException("Incompatible group password!");
        } else if (!joinerType.equals(other.joinerType)) {
            throw new HazelcastException("Incompatible joiners! " + joinerType + " -vs- " + other.joinerType);
        } else if (!partitionGroupEnabled && other.partitionGroupEnabled
                || partitionGroupEnabled && !other.partitionGroupEnabled) {
            throw new HazelcastException("Incompatible partition groups! "
                    + "this: " + (partitionGroupEnabled ? "enabled" : "disabled") + " / " + memberGroupType
                    + ", other: " + (other.partitionGroupEnabled ? "enabled" : "disabled")
                    + " / " + other.memberGroupType);
        } else if (partitionGroupEnabled && memberGroupType != other.memberGroupType) {
            throw new HazelcastException("Incompatible partition groups! this: " + memberGroupType + ", other: "
                    + other.memberGroupType);
        }
        return true;
    }

    public ConfigCheck setGroupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    public ConfigCheck setGroupPassword(String groupPassword) {
        this.groupPassword = groupPassword;
        return this;
    }

    public ConfigCheck setJoinerType(String joinerType) {
        this.joinerType = joinerType;
        return this;
    }

    public ConfigCheck setPartitionGroupEnabled(boolean partitionGroupEnabled) {
        this.partitionGroupEnabled = partitionGroupEnabled;
        return this;
    }

    public ConfigCheck setMemberGroupType(PartitionGroupConfig.MemberGroupType memberGroupType) {
        this.memberGroupType = memberGroupType;
        return this;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.CONFIG_CHECK;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(groupName);
        out.writeUTF(groupPassword);
        out.writeUTF(joinerType);
        out.writeBoolean(partitionGroupEnabled);
        if (partitionGroupEnabled) {
            out.writeUTF(memberGroupType.toString());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupName = in.readUTF();
        groupPassword = in.readUTF();
        joinerType = in.readUTF();
        partitionGroupEnabled = in.readBoolean();
        if (partitionGroupEnabled) {
            String s = in.readUTF();
            try {
                memberGroupType = PartitionGroupConfig.MemberGroupType.valueOf(s);
            } catch (IllegalArgumentException ignored) {
                ignore(ignored);
            }
        }
    }
}
