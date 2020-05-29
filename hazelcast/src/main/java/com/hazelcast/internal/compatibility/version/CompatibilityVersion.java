/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.compatibility.version;

import com.hazelcast.internal.compatibility.cluster.impl.CompatibilityClusterDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * A compatibility (4.x) version object.
 * For all intents and purposes, it is equivalent to {@link com.hazelcast.version.Version}.
 */
@SuppressWarnings("checkstyle:magicnumber")
public final class CompatibilityVersion implements IdentifiedDataSerializable, Comparable<CompatibilityVersion> {

    /**
     * Use 0 as major &amp; minor values for UNKNOWN version
     */
    public static final byte UNKNOWN_VERSION = 0;

    /**
     * Version 0.0 is UNKNOWN constant
     * <ul>
     * <li>UNKNOWN is only equal to itself.</li>
     * <li>{@code is(Less|Greater)Than} method with an UNKNOWN operand returns false.</li>
     * <li>{@code is(Less|Greater)OrEqual} with an UNKNOWN operand returns false, unless both operands are UNKNOWN.</li>
     * <li>{@code UNKNOWN.isUnknown(Less|Greater)(Than|OrEqual)} returns true.</li>
     * <li>{@code otherVersion.isUnknown(Less|Greater)(Than|OrEqual)} with an UNKNOWN argument returns false.</li>
     * </ul>
     */
    public static final CompatibilityVersion UNKNOWN
            = new CompatibilityVersion(UNKNOWN_VERSION, UNKNOWN_VERSION);

    private byte major;
    private byte minor;

    public CompatibilityVersion() {
    }

    private CompatibilityVersion(int major, int minor) {
        assert major >= 0 && major <= Byte.MAX_VALUE : "Invalid value: " + major + ", must be in range [0,127]";
        assert minor >= 0 && minor <= Byte.MAX_VALUE : "Invalid value: " + minor + ", must be in range [0,127]";

        this.major = (byte) major;
        this.minor = (byte) minor;
    }

    public byte getMajor() {
        return major;
    }

    public byte getMinor() {
        return minor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompatibilityVersion that = (CompatibilityVersion) o;
        return isEqualTo(that);
    }

    @Override
    public int hashCode() {
        int result = (int) major;
        result = 31 * result + (int) minor;
        return result;
    }

    // this method is used when serializing Version in JSON response for Management Center
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
    public int compareTo(CompatibilityVersion o) {
        int thisVersion = this.pack();
        int thatVersion = o.pack();
        // min pack value is 0
        // max pack value is lower than Short.MAX
        return thisVersion - thatVersion;
    }

    @Override
    public int getFactoryId() {
        return CompatibilityClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CompatibilityClusterDataSerializerHook.VERSION;
    }

    /**
     * @return the {@code ClusterVersion} with the given major and minor
     */
    public static CompatibilityVersion of(int major, int minor) {
        if (major == UNKNOWN_VERSION && minor == UNKNOWN_VERSION) {
            return UNKNOWN;
        } else {
            return new CompatibilityVersion(major, minor);
        }
    }

    /**
     * @param version other version to compare to
     * @return {@code true} if this version equals {@code version}
     */
    public boolean isEqualTo(CompatibilityVersion version) {
        return major == version.major && minor == version.minor;
    }

    /**
     * @return a packed integer representation of this Version
     */
    private int pack() {
        return (major << 8 & 0xff00) | (minor & 0xff);
    }
}
