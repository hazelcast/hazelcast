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

import com.hazelcast.util.EmptyStatement;

import java.io.InputStream;
import java.util.Properties;

/**
 * Provides information about current Hazelcast build.
 */
public final class BuildInfoProvider {

    private BuildInfoProvider() {
    }

    public static BuildInfo getBuildInfo() {
        final InputStream inRuntimeProperties =
                BuildInfoProvider.class.getClassLoader().getResourceAsStream("hazelcast-runtime.properties");
        Properties runtimeProperties = new Properties();
        try {
            if (inRuntimeProperties != null) {
                runtimeProperties.load(inRuntimeProperties);
                inRuntimeProperties.close();
            }
        } catch (Exception ignored) {
            EmptyStatement.ignore(ignored);
        }

        String version = runtimeProperties.getProperty("hazelcast.version");
        String distribution = runtimeProperties.getProperty("hazelcast.distribution");
        String revision = runtimeProperties.getProperty("hazelcast.git.revision", "");
        if (!revision.isEmpty() && revision.equals("${git.commit.id.abbrev}")) {
            revision = "";
        }
        boolean enterprise = !"Hazelcast".equals(distribution);

        // override BUILD_NUMBER with a system property
        String build;
        Integer hazelcastBuild = Integer.getInteger("hazelcast.build", -1);
        if (hazelcastBuild == -1) {
            build = runtimeProperties.getProperty("hazelcast.build");
        } else {
            build = String.valueOf(hazelcastBuild);
        }
        int buildNumber = Integer.parseInt(build);

        return new BuildInfo(version, build, revision, buildNumber, enterprise);
    }

}
