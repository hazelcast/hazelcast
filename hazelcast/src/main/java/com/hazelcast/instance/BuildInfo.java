/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

public class BuildInfo {

    private final String version;
    private final String build;
    private final int buildNumber;
    private final boolean enterprise;

    public BuildInfo(String version, String build, int buildNumber, boolean enterprise) {
        this.version = version;
        this.build = build;
        this.buildNumber = buildNumber;
        this.enterprise = enterprise;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BuildInfo{");
        sb.append("version='").append(version).append('\'');
        sb.append(", build='").append(build).append('\'');
        sb.append(", buildNumber=").append(buildNumber);
        sb.append(", enterprise=").append(enterprise);
        sb.append('}');
        return sb.toString();
    }
}
