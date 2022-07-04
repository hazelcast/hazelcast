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

package com.hazelcast.instance;

import com.hazelcast.logging.Logger;

import static com.hazelcast.internal.util.StringUtil.tokenizeVersionString;
import static java.lang.Integer.parseInt;

public class BuildInfo {

    public static final int UNKNOWN_HAZELCAST_VERSION = -1;

    // major.minor.patch-RC-SNAPSHOT
    private static final int MAJOR_VERSION_MULTIPLIER = 10000;
    private static final int MINOR_VERSION_MULTIPLIER = 100;
    private static final int PATCH_TOKEN_INDEX = 3;

    private final String version;
    private final String build;
    private final String revision;
    private final int buildNumber;
    private final boolean enterprise;
    private final byte serializationVersion;
    private final BuildInfo upstreamBuildInfo;
    private final String commitId;

    public BuildInfo(String version, String build, String revision, int buildNumber, boolean enterprise,
                     byte serializationVersion, String commitId) {
        this(version, build, revision, buildNumber, enterprise, serializationVersion, commitId, null);
    }

    public BuildInfo(String version, String build, String revision, int buildNumber, boolean enterprise,
                     byte serializationVersion, String commitId, BuildInfo upstreamBuildInfo) {
        this.version = version;
        this.build = build;
        this.revision = revision;
        this.buildNumber = buildNumber;
        this.enterprise = enterprise;
        this.serializationVersion = serializationVersion;
        this.commitId = commitId;
        this.upstreamBuildInfo = upstreamBuildInfo;
    }

    public String getRevision() {
        return revision;
    }

    public String getVersion() {
        return version;
    }

    public String getBuild() {
        return build;
    }

    public int getBuildNumber() {
        return buildNumber;
    }

    public boolean isEnterprise() {
        return enterprise;
    }

    public byte getSerializationVersion() {
        return serializationVersion;
    }

    public BuildInfo getUpstreamBuildInfo() {
        return upstreamBuildInfo;
    }

    public String getCommitId() {
        return commitId;
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
                + (upstreamBuildInfo == null ? "" : ", upstream=" + upstreamBuildInfo)
                + '}';
    }

    public static int calculateVersion(String version) {
        if (null == version) {
            return UNKNOWN_HAZELCAST_VERSION;
        }

        String[] versionTokens = tokenizeVersionString(version);
        if (versionTokens != null) {
            try {
                int calculatedVersion = MAJOR_VERSION_MULTIPLIER * parseInt(versionTokens[0])
                        + MINOR_VERSION_MULTIPLIER * parseInt(versionTokens[1]);
                int groupCount = versionTokens.length;
                if (groupCount >= PATCH_TOKEN_INDEX) {
                    String patchVersionString = versionTokens[PATCH_TOKEN_INDEX];
                    if (null != patchVersionString && !patchVersionString.startsWith("-")) {
                        calculatedVersion += parseInt(patchVersionString);
                    }
                }
                return calculatedVersion;
            } catch (Exception e) {
                Logger.getLogger(BuildInfo.class).warning("Failed to calculate version using version string " + version, e);
            }
        }

        return UNKNOWN_HAZELCAST_VERSION;
    }
}
