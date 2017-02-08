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

package com.hazelcast.version;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.StringUtil;

import java.io.IOException;

/**
 * Represents the cluster version in terms of major.minor version.
 *
 * @since 3.8
 */
@SuppressWarnings("checkstyle:magicnumber")
public final class ClusterVersion implements IdentifiedDataSerializable, Comparable<ClusterVersion> {

    /**
     * Unknown cluster version
     */
    public static final ClusterVersion UNKNOWN = new ClusterVersion(0, 0);

    private byte major;
    private byte minor;

    public ClusterVersion() {
    }

    public ClusterVersion(int major, int minor) {
        this.major = (byte) major;
        this.minor = (byte) minor;
    }

    public byte getMajor() {
        return major;
    }

    public byte getMinor() {
        return minor;
    }

    public boolean isEqualTo(ClusterVersion version) {
        return this.equals(version);
    }

    public boolean isLessThan(ClusterVersion version) {
        return this.major < version.major || this.major == version.major && this.minor < version.minor;
    }

    public boolean isLessOrEqual(ClusterVersion version) {
        return isLessThan(version) || isEqualTo(version);
    }

    public boolean isGreaterThan(ClusterVersion version) {
        return this.major > version.major || this.major == version.major && this.minor > version.minor;
    }

    public boolean isGreaterOrEqual(ClusterVersion version) {
        return isGreaterThan(version) || isEqualTo(version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterVersion that = (ClusterVersion) o;
        if (major != that.major) {
            return false;
        }
        return minor == that.minor;
    }

    @Override
    public int hashCode() {
        int result = (int) major;
        result = 31 * result + (int) minor;
        return result;
    }

    // this method is also used when serializing this ClusterVersion inside JSON response for Management Center
    @Override
    public String toString() {
        return major + "." + minor;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(major);
        out.writeByte(minor);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.major = in.readByte();
        this.minor = in.readByte();
    }

    @Override
    public int compareTo(ClusterVersion o) {
        int thisVersion = (this.major << 8 & 0xff00) | (this.minor & 0xff);
        int thatVersion = (o.major << 8 & 0xff00) | (o.minor & 0xff);
        if (thisVersion > thatVersion) {
            return 1;
        } else {
            return thisVersion == thatVersion ? 0 : -1;
        }
    }

    public boolean isUnknown() {
        return this.major == 0 && this.minor == 0;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.CLUSTER_VERSION;
    }

    /**
     * @return the {@code ClusterVersion} with the given major and minor
     */
    public static ClusterVersion of(int major, int minor) {
        return new ClusterVersion(major, minor);
    }

    /**
     * Parse the given string to a {@code ClusterVersion}. This method may throw an {@code IllegalArgumentException}
     *
     * @param version string to parse to {@code ClusterVersion}.
     * @return the {@code ClusterVersion} parsed from given argument.
     */
    public static ClusterVersion of(String version) {
        String[] tokens = StringUtil.tokenizeVersionString(version);
        if (tokens != null && tokens.length >= 2) {
            return new ClusterVersion(Byte.valueOf(tokens[0]), Byte.valueOf(tokens[1]));
        } else {
            throw new IllegalArgumentException("Cannot parse " + version + " to ClusterVersion.");
        }
    }

}
