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
import com.hazelcast.util.StringUtil;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.io.Serializable;

/**
 * A compatibility (4.x) member version object.
 * For all intents and purposes, it is equivalent to {@link com.hazelcast.version.MemberVersion}.
 */
@SuppressWarnings("checkstyle:magicnumber")
public final class CompatibilityMemberVersion
        implements IdentifiedDataSerializable, Serializable, Comparable<CompatibilityMemberVersion> {

    /**
     * UNKNOWN version.
     */
    public static final CompatibilityMemberVersion UNKNOWN = new CompatibilityMemberVersion(0, 0, 0);

    private static final String UNKNOWN_VERSION_STRING = "0.0.0";
    private static final long serialVersionUID = 2603770920931610781L;

    private byte major;
    private byte minor;
    private byte patch;

    public CompatibilityMemberVersion() {
    }

    public CompatibilityMemberVersion(int major, int minor, int patch) {
        this.major = (byte) major;
        this.minor = (byte) minor;
        this.patch = (byte) patch;
    }

    public CompatibilityMemberVersion(String version) {
        parse(version);
    }

    // populate this Version's major, minor, patch from given String
    private void parse(String version) {
        String[] tokens = StringUtil.tokenizeVersionString(version);
        this.major = Byte.valueOf(tokens[0]);
        this.minor = Byte.valueOf(tokens[1]);
        if (tokens.length > 3 && tokens[3] != null) {
            this.patch = Byte.valueOf(tokens[3]);
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
        CompatibilityMemberVersion that = (CompatibilityMemberVersion) o;
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
        int result = (int) major;
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

    public static CompatibilityMemberVersion of(int major, int minor, int patch) {
        if (major == 0 && minor == 0 && patch == 0) {
            return CompatibilityMemberVersion.UNKNOWN;
        } else {
            return new CompatibilityMemberVersion(major, minor, patch);
        }
    }

    public static CompatibilityMemberVersion of(String version) {
        if (version == null || version.startsWith(UNKNOWN_VERSION_STRING)) {
            return CompatibilityMemberVersion.UNKNOWN;
        } else {
            return new CompatibilityMemberVersion(version);
        }
    }

    @Override
    public int getFactoryId() {
        return CompatibilityClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CompatibilityClusterDataSerializerHook.MEMBER_VERSION;
    }

    @Override
    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    public int compareTo(CompatibilityMemberVersion otherVersion) {
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
    public Version asVersion() {
        return Version.of(major, minor);
    }
}
