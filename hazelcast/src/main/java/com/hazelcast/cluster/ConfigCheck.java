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

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.instance.GroupProperties.PROP_APPLICATION_VALIDATION_TOKEN;
import static com.hazelcast.instance.GroupProperties.PROP_PARTITION_COUNT;

/**
 * Contains enough information about Hazelcast Config, to do a check when a cluster joins.
 */
public final class ConfigCheck implements IdentifiedDataSerializable {

    private String groupName;

    private String groupPassword;

    private String joinerType;

    private boolean partitionGroupEnabled;

    private PartitionGroupConfig.MemberGroupType memberGroupType;

    private Map<String, String> properties = new HashMap<String, String>();

    public ConfigCheck() {
    }

    public ConfigCheck(Config config, String joinerType) {
        this.joinerType = joinerType;

        // Copying all properties relevant for checking.
        this.properties.put(PROP_PARTITION_COUNT, config.getProperty(PROP_PARTITION_COUNT));
        this.properties.put(PROP_APPLICATION_VALIDATION_TOKEN, config.getProperty(PROP_APPLICATION_VALIDATION_TOKEN));

        // Copying group-config settings/
        GroupConfig groupConfig = config.getGroupConfig();
        if (groupConfig != null) {
            this.groupName = groupConfig.getName();
            this.groupPassword = config.getGroupConfig().getPassword();
        }

        // Partition-group settings
        final PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        if (partitionGroupConfig != null) {
            partitionGroupEnabled = partitionGroupConfig.isEnabled();
            if (partitionGroupEnabled) {
                memberGroupType = partitionGroupConfig.getGroupType();
            } else {
                memberGroupType = PartitionGroupConfig.MemberGroupType.PER_MEMBER;
            }
        }
    }

    public boolean isCompatible(ConfigCheck that) {
        // check group-properties.
        if (!equals(groupName, that.groupName)) {
            return false;
        }
        if (!equals(groupPassword, that.groupPassword)) {
            throw new ConfigMismatchException("Incompatible group password!");
        }

        verifyJoiner(that);
        verifyPartitionGroup(that);
        verifyPartitionCount(that);
        verifyApplicationValidationToken(that);
        return true;
    }

    private void verifyApplicationValidationToken(ConfigCheck other) {
        String thisValidationToken = properties.get(PROP_APPLICATION_VALIDATION_TOKEN);
        String thatValidationToken = other.properties.get(PROP_APPLICATION_VALIDATION_TOKEN);
        if (!equals(thisValidationToken, thatValidationToken)) {
            throw new ConfigMismatchException("Incompatible '" + PROP_APPLICATION_VALIDATION_TOKEN + "'! this: " +
                    thisValidationToken + ", other: " + thatValidationToken);
        }
    }

    private void verifyPartitionCount(ConfigCheck other) {
        String thisPartitionCount = properties.get(PROP_PARTITION_COUNT);
        String thatPartitionCount = other.properties.get(PROP_PARTITION_COUNT);
        if (!equals(thisPartitionCount, thatPartitionCount)) {
            throw new ConfigMismatchException("Incompatible partition count! this: " + thisPartitionCount + ", other: "
                    + thatPartitionCount);
        }
    }

    private void verifyPartitionGroup(ConfigCheck other) {
        if (!partitionGroupEnabled && other.partitionGroupEnabled
                || partitionGroupEnabled && !other.partitionGroupEnabled) {
            throw new ConfigMismatchException("Incompatible partition groups! "
                    + "this: " + (partitionGroupEnabled ? "enabled" : "disabled") + " / " + memberGroupType
                    + ", other: " + (other.partitionGroupEnabled ? "enabled" : "disabled")
                    + " / " + other.memberGroupType);
        }

        if (partitionGroupEnabled && memberGroupType != other.memberGroupType) {
            throw new ConfigMismatchException("Incompatible partition groups! this: " + memberGroupType + ", other: "
                    + other.memberGroupType);
        }
    }

    private void verifyJoiner(ConfigCheck other) {
        if (!equals(joinerType, other.joinerType)) {
            throw new ConfigMismatchException("Incompatible joiners! " + joinerType + " -vs- " + other.joinerType);
        }
    }

    private static boolean equals(String thisValue, String thatValue) {
        if (thisValue == null) {
            return thatValue == null;
        }

        return thisValue.equals(thatValue);
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
        out.writeObject(properties);
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
            }
        }
        properties = in.readObject();
    }
}
