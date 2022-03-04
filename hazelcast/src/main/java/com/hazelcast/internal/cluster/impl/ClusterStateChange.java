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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.version.Version;

import java.io.IOException;

/**
 * Wrapper object indicating a change in the cluster state - it may be cluster state change or cluster version change.
 */
public class ClusterStateChange<T> implements IdentifiedDataSerializable {

    private Class<T> type;
    private T newState;

    public ClusterStateChange() {
    }

    @SuppressWarnings("unchecked")
    public ClusterStateChange(T newState) {
        this.type = (Class<T>) newState.getClass();
        this.newState = newState;
    }

    public Class<T> getType() {
        return type;
    }

    public T getNewState() {
        return newState;
    }

    public <T_SUGGESTED> boolean isOfType(Class<T_SUGGESTED> type) {
        return this.type.equals(type);
    }

    public ClusterState getClusterStateOrNull() {
        return isOfType(ClusterState.class) ? (ClusterState) newState : null;
    }

    public void validate() {
        if (type == null || newState == null) {
            throw new IllegalArgumentException("Invalid null state");
        }

        if (isOfType(Version.class)) {
            if (((Version) newState).isUnknown()) {
                throw new IllegalArgumentException("Cannot change Version to UNKNOWN!");
            }
        }

        if (isOfType(ClusterState.class)) {
            if (newState == ClusterState.IN_TRANSITION) {
                throw new IllegalArgumentException("IN_TRANSITION is an internal state!");
            }
        }
    }

    public static <T> ClusterStateChange<T> from(T object) {
        return new ClusterStateChange<T>(object);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(type);
        out.writeObject(newState);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = in.readObject();
        newState = in.readObject();
    }

    @Override
    public String toString() {
        return "ClusterStateChange{"
                + "type=" + type
                + ", newState=" + newState
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.CLUSTER_STATE_CHANGE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClusterStateChange<?> that = (ClusterStateChange<?>) o;

        if (!type.equals(that.type)) {
            return false;
        }
        return newState.equals(that.newState);

    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + newState.hashCode();
        return result;
    }
}
