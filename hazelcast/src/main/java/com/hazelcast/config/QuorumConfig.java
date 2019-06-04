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

package com.hazelcast.config;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.quorum.QuorumFunction;
import com.hazelcast.quorum.QuorumType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.quorum.QuorumType.READ_WRITE;

/**
 * Configuration for cluster quorum, a means to protect consistency of data from network partitions.
 * In this context, quorum does not refer to an implementation of a consensus protocol, it refers to
 * the number of members in the cluster required for an operation to succeed.
 * <p>
 * Since Hazelcast 3.5, the default built-in quorum implementation keeps track of the number of members
 * in the cluster, as determined by Hazelcast's cluster membership management.
 * <p>
 * Since Hazelcast 3.10, two additional built-in quorum implementations, decoupled from the existing
 * cluster membership management, are provided:
 * <ul>
 *     <li>Probabilistic quorum: in this mode, member heartbeats are tracked and an adaptive failure
 *     detector determines for each member the suspicion level. Additionally, when the Hazelcast member
 *     is configured with the ICMP ping failure detector enabled and operating in parallel mode,
 *     ping information is also used to detect member failures early.
 *     <p>To create a {@code QuorumConfig} for probabilistic quorum, use
 *     {@link #newProbabilisticQuorumConfigBuilder(String, int)} to configure and build the {@code QuorumConfig}.
 *     </li>
 *     <li>Recently-active quorum: in this mode, for a member to be considered present for quorum,
 *     a heartbeat must be received within the configured time-window since now. Additionally, when the
 *     Hazelcast member is configured with the ICMP ping failure detector enabled and operating in
 *     parallel mode, ping information is also used to detect member failures early.
 *     <p>To create a {@code QuorumConfig} for recently-active quorum, use
 *     {@link #newRecentlyActiveQuorumConfigBuilder(String, int, int)} to configure and build the
 *     {@code QuorumConfig}.
 *     </li>
 * </ul>
 *
 * @see QuorumFunction
 * @see com.hazelcast.quorum.impl.ProbabilisticQuorumFunction
 * @see com.hazelcast.quorum.impl.RecentlyActiveQuorumFunction
 */
public class QuorumConfig implements IdentifiedDataSerializable, NamedConfig {

    private String name;
    private boolean enabled;
    private int size;
    private List<QuorumListenerConfig> listenerConfigs = new ArrayList<QuorumListenerConfig>();
    private QuorumType type = READ_WRITE;
    private String quorumFunctionClassName;
    private QuorumFunction quorumFunctionImplementation;

    public QuorumConfig() {
    }

    public QuorumConfig(String name, boolean enabled) {
        this.name = name;
        this.enabled = enabled;
    }

    public QuorumConfig(String name, boolean enabled, int size) {
        this.name = name;
        this.enabled = enabled;
        this.size = size;
    }

    public QuorumConfig(QuorumConfig quorumConfig) {
        this.name = quorumConfig.name;
        this.enabled = quorumConfig.enabled;
        this.size = quorumConfig.size;
        this.listenerConfigs = quorumConfig.listenerConfigs;
        this.type = quorumConfig.type;
    }

    public String getName() {
        return name;
    }

    public QuorumConfig setName(String name) {
        this.name = name;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public QuorumConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public int getSize() {
        return size;
    }

    public QuorumConfig setSize(int size) {
        if (size < 2) {
            throw new InvalidConfigurationException("Minimum quorum size cannot be less than 2");
        }
        this.size = size;
        return this;
    }

    public QuorumType getType() {
        return type;
    }

    public QuorumConfig setType(QuorumType type) {
        this.type = type;
        return this;
    }

    public List<QuorumListenerConfig> getListenerConfigs() {
        return listenerConfigs;
    }


    public QuorumConfig setListenerConfigs(List<QuorumListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return this;
    }

    public QuorumConfig addListenerConfig(QuorumListenerConfig listenerConfig) {
        this.listenerConfigs.add(listenerConfig);
        return this;
    }

    public String getQuorumFunctionClassName() {
        return quorumFunctionClassName;
    }

    public QuorumConfig setQuorumFunctionClassName(String quorumFunctionClassName) {
        this.quorumFunctionClassName = quorumFunctionClassName;
        return this;
    }

    public QuorumFunction getQuorumFunctionImplementation() {
        return quorumFunctionImplementation;
    }

    public QuorumConfig setQuorumFunctionImplementation(QuorumFunction quorumFunctionImplementation) {
        this.quorumFunctionImplementation = quorumFunctionImplementation;
        return this;
    }

    @Override
    public String toString() {
        return "QuorumConfig{"
                + "name='" + name + '\''
                + ", enabled=" + enabled
                + ", size=" + size
                + ", listenerConfigs=" + listenerConfigs
                + ", quorumFunctionClassName=" + quorumFunctionClassName
                + ", quorumFunctionImplementation=" + quorumFunctionImplementation
                + ", type=" + type + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.QUORUM_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
        out.writeBoolean(enabled);
        out.writeInt(size);
        writeNullableList(listenerConfigs, out);
        out.writeUTF(type.name());
        out.writeUTF(quorumFunctionClassName);
        out.writeObject(quorumFunctionImplementation);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        name = in.readUTF();
        enabled = in.readBoolean();
        size = in.readInt();
        listenerConfigs = readNullableList(in);
        type = QuorumType.valueOf(in.readUTF());
        quorumFunctionClassName = in.readUTF();
        quorumFunctionImplementation = in.readObject();
    }

    /**
     * Returns a builder for {@link QuorumConfig} with the given {@code name} using a probabilistic quorum function,
     * for the given quorum {@code size} that is enabled by default.
     *
     * @param name  the quorum's name
     * @param size  minimum count of members for quorum to be considered present
     * @see com.hazelcast.quorum.impl.ProbabilisticQuorumFunction
     */
    public static ProbabilisticQuorumConfigBuilder newProbabilisticQuorumConfigBuilder(String name, int size) {
        return new ProbabilisticQuorumConfigBuilder(name, size);
    }

    /**
     * Returns a builder for a {@link QuorumConfig} with the given {@code name} using a recently-active
     * quorum function for the given quorum {@code size} that is enabled by default.
     * @param name              the quorum's name
     * @param size              minimum count of members for quorum to be considered present
     * @param toleranceMillis   maximum amount of milliseconds that may have passed since last heartbeat was received for a
     *                          member to be considered present for quorum.
     * @see com.hazelcast.quorum.impl.RecentlyActiveQuorumFunction
     */
    public static RecentlyActiveQuorumConfigBuilder newRecentlyActiveQuorumConfigBuilder(String name, int size,
                                                                                         int toleranceMillis) {
        return new RecentlyActiveQuorumConfigBuilder(name, size, toleranceMillis);
    }

}
