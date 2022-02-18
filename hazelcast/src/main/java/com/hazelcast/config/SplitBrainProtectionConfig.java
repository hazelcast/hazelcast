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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionFunction;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.splitbrainprotection.impl.ProbabilisticSplitBrainProtectionFunction;
import com.hazelcast.splitbrainprotection.impl.RecentlyActiveSplitBrainProtectionFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ_WRITE;

/**
 * Configuration for cluster split brain protection, a means to protect consistency of data from network
 * partitions. In this context, split brain protection refers to the number of members in the cluster required
 * for an operation to succeed.
 * <p>
 * Since Hazelcast 3.5, the default built-in split brain protection implementation keeps track of the number of members
 * in the cluster, as determined by Hazelcast's cluster membership management.
 * <p>
 * Since Hazelcast 3.10, two additional built-in split brain protection implementations, decoupled from the existing
 * cluster membership management, are provided:
 * <ul>
 *     <li>Probabilistic split brain protection: in this mode, member heartbeats are tracked and an adaptive failure
 *     detector determines for each member the suspicion level. Additionally, when the Hazelcast member
 *     is configured with the ICMP ping failure detector enabled and operating in parallel mode,
 *     ping information is also used to detect member failures early.
 *     <p>To create a {@code SplitBrainProtectionConfig} for probabilistic split brain protection, use
 *     {@link #newProbabilisticSplitBrainProtectionConfigBuilder(String, int)} to configure and build the
 *     {@code SplitBrainProtectionConfig}.
 *     </li>
 *     <li>Recently-active split brain protection: in this mode, for a member to be considered present for split brain
 *     protection, a heartbeat must be received within the configured time-window since now. Additionally, when the
 *     Hazelcast member is configured with the ICMP ping failure detector enabled and operating in
 *     parallel mode, ping information is also used to detect member failures early.
 *     <p>To create a {@code SplitBrainProtectionConfig} for recently-active split brain protection, use
 *     {@link #newRecentlyActiveSplitBrainProtectionConfigBuilder(String, int, int)} to configure and build the
 *     {@code SplitBrainProtectionConfig}.
 *     </li>
 * </ul>
 *
 * @see SplitBrainProtectionFunction
 * @see ProbabilisticSplitBrainProtectionFunction
 * @see RecentlyActiveSplitBrainProtectionFunction
 */
public class SplitBrainProtectionConfig implements IdentifiedDataSerializable, NamedConfig {

    private String name;
    private boolean enabled;
    private int minimumClusterSize = 2;

    private List<SplitBrainProtectionListenerConfig> listenerConfigs = new ArrayList<>();

    private SplitBrainProtectionOn protectOn = READ_WRITE;
    private String functionClassName;
    private SplitBrainProtectionFunction functionImplementation;

    public SplitBrainProtectionConfig() {
    }

    public SplitBrainProtectionConfig(String name) {
        this.name = name;
    }

    public SplitBrainProtectionConfig(String name, boolean enabled) {
        this.name = name;
        this.enabled = enabled;
    }

    public SplitBrainProtectionConfig(String name, boolean enabled, int minimumClusterSize) {
        this.name = name;
        this.enabled = enabled;
        this.minimumClusterSize = minimumClusterSize;
    }

    public SplitBrainProtectionConfig(SplitBrainProtectionConfig splitBrainProtectionConfig) {
        this.name = splitBrainProtectionConfig.name;
        this.enabled = splitBrainProtectionConfig.enabled;
        this.minimumClusterSize = splitBrainProtectionConfig.minimumClusterSize;
        this.listenerConfigs = splitBrainProtectionConfig.listenerConfigs;
        this.protectOn = splitBrainProtectionConfig.protectOn;
    }

    public String getName() {
        return name;
    }

    public SplitBrainProtectionConfig setName(String name) {
        this.name = name;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public SplitBrainProtectionConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public int getMinimumClusterSize() {
        return minimumClusterSize;
    }

    public SplitBrainProtectionConfig setMinimumClusterSize(int minimumClusterSize) {
        if (minimumClusterSize < 2) {
            throw new InvalidConfigurationException("Minimum cluster size configured for split-brain protection"
                    + " cannot be less than 2");
        }
        this.minimumClusterSize = minimumClusterSize;
        return this;
    }

    public SplitBrainProtectionOn getProtectOn() {
        return protectOn;
    }

    public SplitBrainProtectionConfig setProtectOn(SplitBrainProtectionOn protectOn) {
        this.protectOn = protectOn;
        return this;
    }

    public List<SplitBrainProtectionListenerConfig> getListenerConfigs() {
        return listenerConfigs;
    }


    public SplitBrainProtectionConfig setListenerConfigs(List<SplitBrainProtectionListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return this;
    }

    public SplitBrainProtectionConfig addListenerConfig(SplitBrainProtectionListenerConfig listenerConfig) {
        this.listenerConfigs.add(listenerConfig);
        return this;
    }

    public String getFunctionClassName() {
        return functionClassName;
    }

    public SplitBrainProtectionConfig setFunctionClassName(String functionClassName) {
        this.functionClassName = functionClassName;
        return this;
    }

    public SplitBrainProtectionFunction getFunctionImplementation() {
        return functionImplementation;
    }

    public SplitBrainProtectionConfig
    setFunctionImplementation(SplitBrainProtectionFunction functionImplementation) {
        this.functionImplementation = functionImplementation;
        return this;
    }

    @Override
    public String toString() {
        return "SplitBrainProtectionConfig{"
                + "name='" + name + '\''
                + ", enabled=" + enabled
                + ", minimumClusterSize=" + minimumClusterSize
                + ", listenerConfigs=" + listenerConfigs
                + ", functionClassName=" + functionClassName
                + ", functionImplementation=" + functionImplementation
                + ", protectOn=" + protectOn + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.SPLIT_BRAIN_PROTECTION_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeString(name);
        out.writeBoolean(enabled);
        out.writeInt(minimumClusterSize);
        writeNullableList(listenerConfigs, out);
        out.writeString(protectOn.name());
        out.writeString(functionClassName);
        out.writeObject(functionImplementation);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        name = in.readString();
        enabled = in.readBoolean();
        minimumClusterSize = in.readInt();
        listenerConfigs = readNullableList(in);
        protectOn = SplitBrainProtectionOn.valueOf(in.readString());
        functionClassName = in.readString();
        functionImplementation = in.readObject();
    }

    /**
     * Returns a builder for {@link SplitBrainProtectionConfig} with the given {@code name} using a probabilistic
     * split brain protection function, for the given split brain protection {@code size} that is enabled by default.
     *
     * @param name               the split brain protection's name
     * @param minimumClusterSize minimum count of members in the cluster not to be considered it split.
     * @see ProbabilisticSplitBrainProtectionFunction
     */
    public static ProbabilisticSplitBrainProtectionConfigBuilder
    newProbabilisticSplitBrainProtectionConfigBuilder(String name, int minimumClusterSize) {
        return new ProbabilisticSplitBrainProtectionConfigBuilder(name, minimumClusterSize);
    }

    /**
     * Returns a builder for a {@link SplitBrainProtectionConfig} with the given {@code name} using a recently-active
     * split brain protection function for the given split brain protection {@code size} that is enabled by default.
     *
     * @param name               the split brain protection's name
     * @param minimumClusterSize minimum count of members in the cluster not to be considered it split.
     * @param toleranceMillis    maximum amount of milliseconds that may have passed since last heartbeat was received for a
     *                           member to be considered present for split brain protection.
     * @see RecentlyActiveSplitBrainProtectionFunction
     */
    public static RecentlyActiveSplitBrainProtectionConfigBuilder
    newRecentlyActiveSplitBrainProtectionConfigBuilder(String name, int minimumClusterSize, int toleranceMillis) {
        return new RecentlyActiveSplitBrainProtectionConfigBuilder(name, minimumClusterSize, toleranceMillis);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SplitBrainProtectionConfig that = (SplitBrainProtectionConfig) o;
        return enabled == that.enabled && minimumClusterSize == that.minimumClusterSize
                && Objects.equals(name, that.name) && Objects.equals(listenerConfigs, that.listenerConfigs)
                && protectOn == that.protectOn && Objects.equals(functionClassName, that.functionClassName)
                && Objects.equals(functionImplementation, that.functionImplementation);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(name, enabled, minimumClusterSize, listenerConfigs, protectOn, functionClassName, functionImplementation);
    }
}
