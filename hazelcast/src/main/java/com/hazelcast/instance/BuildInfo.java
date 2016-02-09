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

package com.hazelcast.instance;

/**
 * Contains the information of the Hazelcast build.
 */
public class BuildInfo {

    private final String version;
    private final String build;
    private final String revision;
    private final int buildNumber;
    private final boolean enterprise;
    private final byte serializationVersion;

    public BuildInfo(String version, String build, String revision, int buildNumber, boolean enterprise,
                     byte serializationVersion) {
        this.version = version;
        this.build = build;
        this.revision = revision;
        this.buildNumber = buildNumber;
        this.enterprise = enterprise;
        this.serializationVersion = serializationVersion;
    }

    /**
     * Returns the revision.
     *
     * @return the revision.
     */
    public String getRevision() {
        return revision;
    }

    /**
     * Returns the version, e.g. '3.6'.
     *
     * @return the version.
     */
    public String getVersion() {
        return version;
    }

    /**
     * Returns the build.
     *
     * @return the build.
     */
    public String getBuild() {
        return build;
    }

    /**
     * Returns the build number.
     *
     * @return the build number.
     */
    public int getBuildNumber() {
        return buildNumber;
    }

    /**
     * Checks if this is an Enterprise build or not.
     *
     * @return true if enterprise, false otherwise.
     */
    public boolean isEnterprise() {
        return enterprise;
    }

    /**
     * Gets the version of the serialization.
     *
     * @return serialization version.
     */
    public byte getSerializationVersion() {
        return serializationVersion;
    }

    @Override
    public String toString() {
        return "BuildInfo{"
                + "version='" + version + '\''
                + ", build='" + build + '\''
                + ", buildNumber=" + buildNumber
                + ", revision=" + revision
                + ", enterprise=" + enterprise
                + ", serializationVersion=" + serializationVersion
                + '}';
    }
}
