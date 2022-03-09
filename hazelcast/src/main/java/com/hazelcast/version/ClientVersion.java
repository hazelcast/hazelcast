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

import com.hazelcast.internal.util.StringUtil;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * Class to represent the code base version of the client in the form of major.minor.patch
 */
public final class ClientVersion implements Serializable, Comparable<ClientVersion> {

    private static final long serialVersionUID = 1L;

    private final byte major;
    private final byte minor;
    private final byte patch;

    private ClientVersion(int major, int minor, int patch) {
        this.major = (byte) major;
        this.minor = (byte) minor;
        this.patch = (byte) patch;
    }

    /**
     * major minor and patch should be non-negative numbers.
     *
     * @return client version in the form of major.minor.patch
     * @throws IllegalArgumentException if major, minor or patch is negative.
     */
    @Nonnull
    public static ClientVersion of(int major, int minor, int patch) {
        if (major < 0 || minor < 0 || patch < 0) {
            throw new IllegalArgumentException("Major, minor and patch should be non-negative numbers.");
        }
        return new ClientVersion(major, minor, patch);
    }

    /**
     * Creates ClientVersion from given string. Keeps major, minor and patch as is and discards the rest.
     *
     * @param versionString version of the client as string in the form of "major.minor.[patch][-ANY_WORD][-SNAPSHOT]".
     *                      For example: "1.2", "1.0.0", "1.0.0-SNAPSHOT", "1.0.0-RC1", "1.0.0-RC1-SNAPSHOT"
     * @throws IllegalArgumentException if given version string is not in the correct format.
     */
    @Nonnull
    public static ClientVersion of(String versionString) {
        String[] tokens = StringUtil.tokenizeVersionString(versionString);
        if (tokens == null || tokens.length < 2) {
            throw new IllegalArgumentException("Invalid version string: " + versionString);
        }
        int major = Byte.parseByte(tokens[0]);
        int minor = Byte.parseByte(tokens[1]);
        int patch = 0;
        if (tokens.length > 3 && tokens[3] != null) {
            patch = Byte.parseByte(tokens[3]);
        }
        return new ClientVersion(major, minor, patch);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClientVersion that = (ClientVersion) o;
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

    @Override
    public String toString() {
        return major + "." + minor + "." + patch;
    }

    @Override
    public int compareTo(ClientVersion o) {
        if (major != o.major) {
            return major - o.major;
        }
        if (minor != o.minor) {
            return minor - o.minor;
        }
        return patch - o.patch;
    }
}
