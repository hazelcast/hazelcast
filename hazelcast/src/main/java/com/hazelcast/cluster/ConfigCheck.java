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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.instance.GroupProperties.PROP_APPLICATION_VALIDATION_TOKEN;
import static com.hazelcast.instance.GroupProperties.PROP_PARTITION_COUNT;

/**
 * Contains enough information about Hazelcast Config, to do a validation check so that clusters with different configurations
 * don't join.
 */
public final class ConfigCheck implements IdentifiedDataSerializable {

    private String groupName;

    private String groupPassword;

    private String joinerType;

    private boolean partitionGroupEnabled;

    private PartitionGroupConfig.MemberGroupType memberGroupType;

    private Map<String, String> properties = new HashMap<String, String>();

    // TODO: The actual map/queue configuration needs to be added

    private final Map<String, Object> maps = new HashMap<String, Object>();

    private final Map<String, Object> queues = new HashMap<String, Object>();

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

    /**
     * Checks if 2 Hazelcast configurations are compatible.
     *
     * @param found
     * @return true if compatible. False if part of another group.
     * @throws com.hazelcast.cluster.ConfigMismatchException if the configuration isn't compatible. An exception is thrown so
     *                                                       we can pass a nice message.
     */
    public boolean isCompatible(ConfigCheck found) {
        // check group-properties.
        if (!equals(groupName, found.groupName)) {
            return false;
        }

        verifyGroupPassword(found);
        verifyJoiner(found);
        verifyPartitionGroup(found);
        verifyPartitionCount(found);
        verifyApplicationValidationToken(found);
        return true;
    }

    public boolean isSameGroup(ConfigCheck found){
        if (!equals(groupName, found.groupName)) {
            return false;
        }
        return true;
    }

    private void verifyGroupPassword(ConfigCheck found) {
        if (!equals(groupPassword, found.groupPassword)) {
            throw new ConfigMismatchException("Incompatible group password!");
        }
    }

    private void verifyApplicationValidationToken(ConfigCheck found) {
        String expectedValidationToken = properties.get(PROP_APPLICATION_VALIDATION_TOKEN);
        String foundValidationToken = found.properties.get(PROP_APPLICATION_VALIDATION_TOKEN);
        if (!equals(expectedValidationToken, foundValidationToken)) {
            throw new ConfigMismatchException("Incompatible '" + PROP_APPLICATION_VALIDATION_TOKEN + "'! expected: " +
                    expectedValidationToken + ", found: " + foundValidationToken);
        }
    }

    private void verifyPartitionCount(ConfigCheck found) {
        String expectedPartitionCount = properties.get(PROP_PARTITION_COUNT);
        String foundPartitionCount = found.properties.get(PROP_PARTITION_COUNT);
        if (!equals(expectedPartitionCount, foundPartitionCount)) {
            throw new ConfigMismatchException("Incompatible partition count! expected: " + expectedPartitionCount + ", found: "
                    + foundPartitionCount);
        }
    }

    private void verifyPartitionGroup(ConfigCheck found) {
        if (!partitionGroupEnabled && found.partitionGroupEnabled
                || partitionGroupEnabled && !found.partitionGroupEnabled) {
            throw new ConfigMismatchException("Incompatible partition groups! "
                    + "expected: " + (partitionGroupEnabled ? "enabled" : "disabled") + " / " + memberGroupType
                    + ", found: " + (found.partitionGroupEnabled ? "enabled" : "disabled")
                    + " / " + found.memberGroupType);
        }

        if (partitionGroupEnabled && memberGroupType != found.memberGroupType) {
            throw new ConfigMismatchException("Incompatible partition groups! expected: " + memberGroupType + ", found: "
                    + found.memberGroupType);
        }
    }

    private void verifyJoiner(ConfigCheck found) {
        if (!equals(joinerType, found.joinerType)) {
            throw new ConfigMismatchException("Incompatible joiners! expected: " + joinerType + ", found" + found.joinerType);
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

        out.writeInt(maps.size());
        for (Map.Entry<String, Object> entry : maps.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }

        out.writeInt(queues.size());
        for (Map.Entry<String, Object> entry : queues.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
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
            }
        }
        properties = in.readObject();

        int mapSize = in.readInt();
        for (int k = 0; k < mapSize; k++) {
            String key = in.readUTF();
            Object value = in.readObject();
            maps.put(key, value);
        }

        int queueSize = in.readInt();
        for (int k = 0; k < queueSize; k++) {
            String key = in.readUTF();
            Object value = in.readObject();
            queues.put(key, value);
        }
    }
}
