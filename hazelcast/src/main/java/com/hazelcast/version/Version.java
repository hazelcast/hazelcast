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

package com.hazelcast.version;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.VersionAware;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.StringUtil;

import java.io.IOException;

/**
 * A generic version to be used with {@link VersionAware} classes. The version is composed of two bytes,
 * denoting MAJOR.MINOR version. It is used to represent the Hazelcast cluster version and the serialization
 * version of {@link VersionAware} classes.
 *
 * @since 3.8
 */
@SuppressWarnings("checkstyle:magicnumber")
public final class Version implements IdentifiedDataSerializable, Comparable<Version> {

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
    public static final Version UNKNOWN = new Version(UNKNOWN_VERSION, UNKNOWN_VERSION);

    private byte major;
    private byte minor;

    public Version() {
    }

    private Version(int major, int minor) {
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
        Version that = (Version) o;
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
    public int compareTo(Version o) {
        int thisVersion = this.pack();
        int thatVersion = o.pack();
        // min pack value is 0
        // max pack value is lower than Short.MAX
        return thisVersion - thatVersion;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.VERSION;
    }

    /**
     * @return the {@code ClusterVersion} with the given major and minor
     */
    public static Version of(int major, int minor) {
        if (major == UNKNOWN_VERSION && minor == UNKNOWN_VERSION) {
            return UNKNOWN;
        } else {
            return new Version(major, minor);
        }
    }

    /**
     * Parse the given string to a {@code Version}. This method may throw an {@code IllegalArgumentException}
     *
     * @param version string to parse to {@code Version}.
     * @return the {@code Version} parsed from given argument.
     */
    public static Version of(String version) {
        String[] tokens = StringUtil.tokenizeVersionString(version);
        if (tokens != null && tokens.length >= 2) {
            return Version.of(Byte.valueOf(tokens[0]), Byte.valueOf(tokens[1]));
        } else {
            throw new IllegalArgumentException("Cannot parse " + version + " to ClusterVersion.");
        }
    }

    /**
     * @param version other version to compare to
     * @return {@code true} if this version equals {@code version}
     */
    public boolean isEqualTo(Version version) {
        return major == version.major && minor == version.minor;
    }

    /**
     * @param version other version to compare to
     * @return {@code true} if this version is greater than {@code version}
     */
    public boolean isGreaterThan(Version version) {
        return !version.isUnknown() && compareTo(version) > 0;
    }

    /**
     * @param version other version to compare to
     * @return {@code true} if this version is unknown or if this version is greater than {@code version}
     */
    public boolean isUnknownOrGreaterThan(Version version) {
        return isUnknown() || (!version.isUnknown() && compareTo(version) > 0);
    }

    /**
     * @param version other version to compare to
     * @return {@code true} if this version is greater than or equal to {@code version}
     */
    public boolean isGreaterOrEqual(Version version) {
        return (!version.isUnknown() && compareTo(version) >= 0) || (version.isUnknown() && isUnknown());
    }

    /**
     * @param version other version to compare to
     * @return {@code true} if this version is unknown or if this version is greater than or equal to {@code version}
     */
    public boolean isUnknownOrGreaterOrEqual(Version version) {
        return isUnknown() || (!version.isUnknown() && compareTo(version) >= 0);
    }

    /**
     * @param version other version to compare to
     * @return {@code true} if this version is less than {@code version}
     */
    public boolean isLessThan(Version version) {
        return !isUnknown() && compareTo(version) < 0;
    }

    /**
     * @param version other version to compare to
     * @return {@code true} if this version is unknown or if this version is less than {@code version}
     */
    public boolean isUnknownOrLessThan(Version version) {
        return isUnknown() || compareTo(version) < 0;
    }

    /**
     * @param version other version to compare to
     * @return {@code true} if this version is less than or equal to {@code version}
     */
    public boolean isLessOrEqual(Version version) {
        return (!isUnknown() && compareTo(version) <= 0) || (isUnknown() && version.isUnknown());
    }

    /**
     * @param version other version to compare to
     * @return {@code true} if this version is unknown or if this version is less than or equal to {@code version}
     */
    public boolean isUnknownOrLessOrEqual(Version version) {
        return isUnknown() || compareTo(version) <= 0;
    }

    /**
     * Checks if the version is between specified version (both ends inclusive)
     *
     * @param from the lower bound version
     * @param to the upper bound version
     * @return true if the version is between from and to (both ends inclusive)
     */
    public boolean isBetween(Version from, Version to) {
        int thisVersion = this.pack();
        int fromVersion = from.pack();
        int toVersion = to.pack();
        return thisVersion >= fromVersion && thisVersion <= toVersion;
    }

    /**
     * @return {@code true} if this version is equal to {@link Version#UNKNOWN}
     */
    public boolean isUnknown() {
        return pack() == UNKNOWN_VERSION;
    }

    /**
     * @return a packed integer representation of this Version
     */
    private int pack() {
        return (major << 8 & 0xff00) | (minor & 0xff);
    }
}
