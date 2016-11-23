/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

/**
 * Determines Hazelcast version.
 */
@SuppressWarnings("checkstyle:magicnumber")
public final class Version implements IdentifiedDataSerializable, Comparable<Version>, Serializable {


    /**
     * UNKNOWN version
     */
    public static final Version UNKNOWN = new Version(0, 0, 0);

    /**
     * Version comparator that takes into account only major & minor version, disregarding patch version number.
     */
    public static final transient Comparator<Version> MAJOR_MINOR_VERSION_COMPARATOR = new MajorMinorVersionComparator();

    private static final String UNKNOWN_VERSION_STRING = "0.0.0";
    private static final long serialVersionUID = 2603770920931610781L;

    private byte major;
    private byte minor;
    private byte patch;

    public Version() {
    }

    public Version(int major, int minor, int patch) {
        this.major = (byte) major;
        this.minor = (byte) minor;
        this.patch = (byte) patch;
    }

    public Version(String version) {
        parse(version);
    }

    // populate this Version's major, minor, patch from given String
    private void parse(String version) {
        String[] tokens = version.split("\\.");
        this.major = Byte.valueOf(tokens[0]);
        this.minor = Byte.valueOf(stripPostfix(tokens[1]));
        if (tokens.length > 2) {
            this.patch = Byte.valueOf(stripPostfix(tokens[2]));
        }
    }

    private static String stripPostfix(String token) {
        int hyphenIndex = token.indexOf("-");
        return hyphenIndex >= 0 ? token.substring(0, hyphenIndex) : token;
    }

    public byte getMajor() {
        return major;
    }

    public byte getMinor() {
        return minor;
    }

    public byte getPatch() {
        return patch;
    }

    public boolean isUnknown() {
        return this.equals(UNKNOWN);
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
        if (major != that.major) {
            return false;
        }
        if (minor != that.minor) {
            return false;
        }
        return patch == that.patch;
    }

    public String asString() {
        return major + "." + minor + "." + patch;
    }

    @Override
    public int hashCode() {
        int result = (int) major;
        result = 31 * result + (int) minor;
        result = 31 * result + (int) patch;
        return result;
    }

    @Override
    public String toString() {
        return major + "." + minor + "." + patch;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(major);
        out.writeByte(minor);
        out.writeByte(patch);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.major = in.readByte();
        this.minor = in.readByte();
        this.patch = in.readByte();
    }

    public static Version of(int major, int minor, int patch) {
        if (major == 0 && minor == 0 && patch == 0) {
            return Version.UNKNOWN;
        } else {
            return new Version(major, minor, patch);
        }
    }

    public static Version of(String version) {
        if (version == null || version.startsWith(UNKNOWN_VERSION_STRING)) {
            return Version.UNKNOWN;
        } else {
            return new Version(version);
        }
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.VERSION;
    }

    @Override
    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    public int compareTo(Version otherVersion) {
        // pack major-minor-patch to 3 least significant bytes of integer, then compare the integers
        // even though major, minor & patch are not expected to be negative, masking makes sure we avoid sign extension
        int thisVersion = (major << 16 & 0xff0000) | ((minor << 8) & 0xff00) | (patch & 0xff);
        int thatVersion = (otherVersion.major << 16 & 0xff0000) | (otherVersion.minor << 8 & 0xff00)
                | (otherVersion.patch & 0xff);
        if (thisVersion > thatVersion) {
            return 1;
        } else {
            return thisVersion == thatVersion ? 0 : -1;
        }
    }

}
