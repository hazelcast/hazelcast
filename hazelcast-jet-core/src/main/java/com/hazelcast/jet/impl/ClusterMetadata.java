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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.Cluster;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;

public class ClusterMetadata implements IdentifiedDataSerializable {

    private String name;
    private String version;
    private long clusterTime;
    private int state;

    public ClusterMetadata() {
    }

    public ClusterMetadata(String name, Cluster cluster) {
        this.name = name;
        this.version = BuildInfoProvider.getBuildInfo().getJetBuildInfo().getVersion();
        this.state = cluster.getClusterState().ordinal();
        this.clusterTime = cluster.getClusterTime();
    }


    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public ClusterState getState() {
        return ClusterState.values()[state];
    }

    public int getStateOrdinal() {
        return state;
    }

    @Nonnull
    public String getVersion() {
        return version;
    }

    public long getClusterTime() {
        return clusterTime;
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.CLUSTER_METADATA;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setClusterTime(long clusterTime) {
        this.clusterTime = clusterTime;
    }

    public void setState(int state) {
        this.state = state;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(version);
        out.writeInt(state);
        out.writeLong(clusterTime);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        version = in.readUTF();
        state = in.readInt();
        clusterTime = in.readLong();
    }

    @Override
    public String toString() {
        return "ClusterSummary{" +
                "name='" + name + '\'' +
                ", version='" + version + '\'' +
                ", clusterTime=" + clusterTime +
                ", state=" + ClusterState.values()[state] +
                '}';
    }

}
