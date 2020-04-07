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

package com.hazelcast.instance;

public class JetBuildInfo {

    private final String version;
    private final String build;
    private final String revision;

    public JetBuildInfo(String version, String build, String revision) {
        this.version = version;
        this.build = build;
        this.revision = revision;
    }

    public String getVersion() {
        return version;
    }

    public String getBuild() {
        return build;
    }

    public String getRevision() {
        return revision;
    }

    @Override
    public String toString() {
        return "JetBuildInfo{"
                + "version='" + version + '\''
                + ", build='" + build + '\''
                + ", revision='" + revision + '\''
                + '}';
    }
}
