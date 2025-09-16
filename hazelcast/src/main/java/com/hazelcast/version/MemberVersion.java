/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nonnull;

import com.hazelcast.internal.util.StringUtil;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.Comparator;

/**
 * Determines the Hazelcast codebase version in terms of major.minor.patch version.
 *
 * @since 3.8
 */
@SuppressWarnings("checkstyle:magicnumber")
public final class MemberVersion
        implements IdentifiedDataSerializable, Serializable, Comparable<MemberVersion> {

    /**
     * UNKNOWN version.
     */
    public static final MemberVersion UNKNOWN = new MemberVersion(0, 0, 0);

    /**
     * Version comparator that takes into account only major &amp; minor version, disregarding patch version number.
     */
    public static final Comparator<MemberVersion> MAJOR_MINOR_VERSION_COMPARATOR = new MajorMinorVersionComparator();

    private static final String UNKNOWN_VERSION_STRING = "0.0.0";
    @Serial
    private static final long serialVersionUID = 2603770920931610781L;

    private byte major;
    private byte minor;
    private byte patch;

    public MemberVersion() {
    }

    public MemberVersion(int major, int minor, int patch) {
        this.major = (byte) major;
        this.minor = (byte) minor;
        this.patch = (byte) patch;
    }

    public MemberVersion(String version) {
        parse(version);
    }

    // populate this Version's major, minor, patch from given String
    private void parse(String version) {
        String[] tokens = StringUtil.tokenizeVersionString(version);
        assert tokens != null;
        this.major = Byte.parseByte(tokens[0]);
        this.minor = Byte.parseByte(tokens[1]);
        if (tokens.length > 3 && tokens[3] != null) {
            this.patch = Byte.parseByte(tokens[3]);
        }
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
        return this.major == 0 && this.minor == 0 && this.patch == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MemberVersion that = (MemberVersion) o;
        if (major != that.major) {
            return false;
        }
        if (minor != that.minor) {
            return false;
        }
        return patch == that.patch;
    }

    @Override
    public int hashCode() {
        int result = major;
        result = 31 * result + (int) minor;
        result = 31 * result + (int) patch;
        return result;
    }

    // this method is used when serializing the MemberVersion inside JSON response for Management Center
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

    public static MemberVersion of(int major, int minor, int patch) {
        if (major == 0 && minor == 0 && patch == 0) {
            return MemberVersion.UNKNOWN;
        } else {
            return new MemberVersion(major, minor, patch);
        }
    }

    public static MemberVersion of(String version) {
        if (version == null || version.startsWith(UNKNOWN_VERSION_STRING)) {
            return MemberVersion.UNKNOWN;
        } else {
            return new MemberVersion(version);
        }
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MEMBER_VERSION;
    }

    @Override
    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    public int compareTo(MemberVersion otherVersion) {
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

    /**
     * @return a {@link Version} initialized with this {@code MemberVersion}'s major.minor version.
     */
    @Nonnull
    public Version asVersion() {
        return Version.of(major, minor);
    }

    /**
     * @param version other version to compare to
     * @return {@code true} if this version is greater than or equal to {@code version}
     */
    public boolean isGreaterOrEqual(MemberVersion version) {
        return (!version.isUnknown() && compareTo(version) >= 0) || (version.isUnknown() && isUnknown());
    }
}
