/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.spi.properties.GroupProperty.APPLICATION_VALIDATION_TOKEN;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Contains enough information about Hazelcast Config, to do a validation check so that clusters with different configurations
 * don't join.
 */
public final class ConfigCheck implements IdentifiedDataSerializable {

    private static final String EMPTY_PWD = "";

    private String groupName;

    private String groupPassword;

    private String joinerType;

    private boolean partitionGroupEnabled;

    private Version clusterVersion = Version.UNKNOWN;

    private PartitionGroupConfig.MemberGroupType memberGroupType;

    private Map<String, String> properties = new HashMap<String, String>();

    // TODO: The actual map/queue configuration needs to be added

    private final Map<String, Object> maps = new HashMap<String, Object>();

    private final Map<String, Object> queues = new HashMap<String, Object>();

    public ConfigCheck() {
    }

    public ConfigCheck(Config config, String joinerType) {
        this(config, joinerType, Version.UNKNOWN);
    }

    public ConfigCheck(Config config, String joinerType, Version clusterVersion) {
        this.joinerType = joinerType;
        this.clusterVersion = clusterVersion;

        // Copying all properties relevant for checking
        properties.put(PARTITION_COUNT.getName(), config.getProperty(PARTITION_COUNT.getName()));
        properties.put(APPLICATION_VALIDATION_TOKEN.getName(), config.getProperty(APPLICATION_VALIDATION_TOKEN.getName()));

        // Copying group-config settings
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
     * Checks if two Hazelcast configurations are compatible.
     *
     * @param found the {@link ConfigCheck} to compare this to
     * @return true if compatible. False if part of another group.
     * @throws ConfigMismatchException if the configuration is not compatible.
     *                                 An exception is thrown so we can pass a nice message.
     */
    public boolean isCompatible(ConfigCheck found) {
        // check group-properties.
        if (!equals(groupName, found.groupName)) {
            return false;
        }

        verifyJoiner(found);
        verifyPartitionGroup(found);
        verifyPartitionCount(found);
        verifyApplicationValidationToken(found);
        return true;
    }

    public boolean isSameGroup(ConfigCheck found) {
        if (!equals(groupName, found.groupName)) {
            return false;
        }
        return true;
    }

    private void verifyApplicationValidationToken(ConfigCheck found) {
        String expectedValidationToken = properties.get(APPLICATION_VALIDATION_TOKEN.getName());
        String foundValidationToken = found.properties.get(APPLICATION_VALIDATION_TOKEN.getName());
        if (!equals(expectedValidationToken, foundValidationToken)) {
            throw new ConfigMismatchException("Incompatible '" + APPLICATION_VALIDATION_TOKEN + "'! expected: "
                    + expectedValidationToken + ", found: " + foundValidationToken);
        }
    }

    private void verifyPartitionCount(ConfigCheck found) {
        String expectedPartitionCount = properties.get(PARTITION_COUNT.getName());
        String foundPartitionCount = found.properties.get(PARTITION_COUNT.getName());
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
            throw new ConfigMismatchException("Incompatible joiners! expected: " + joinerType + ", found: " + found.joinerType);
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
        //TODO @tkountis remove in 4.0
        if (clusterVersion.isUnknownOrLessThan(Versions.V3_9)) {
            // Rolling upgrades support - allow 3.9 Nodes to join 3.8 cluster -
            // however versions above 3.8.2 already know to ignore the password validation
            out.writeUTF(groupPassword);
        } else {
            out.writeUTF(EMPTY_PWD);
        }
        out.writeUTF(joinerType);
        out.writeBoolean(partitionGroupEnabled);
        if (partitionGroupEnabled) {
            out.writeUTF(memberGroupType.toString());
        }

        out.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }

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
        //TODO groupPassword/ignored - @tkountis remove in 4.0
        in.readUTF();
        joinerType = in.readUTF();
        partitionGroupEnabled = in.readBoolean();
        if (partitionGroupEnabled) {
            String s = in.readUTF();
            try {
                memberGroupType = PartitionGroupConfig.MemberGroupType.valueOf(s);
            } catch (IllegalArgumentException ignored) {
                EmptyStatement.ignore(ignored);
            }
        }
        int propSize = in.readInt();
        properties = createHashMap(propSize);
        for (int k = 0; k < propSize; k++) {
            String key = in.readUTF();
            String value = in.readUTF();
            properties.put(key, value);
        }

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
