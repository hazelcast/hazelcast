/*
 * Copyright (c) 2008, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.EmptyStatement.ignore;

/**
 * Provides information about current Hazelcast build.
 */
public final class BuildInfoProvider {

    /**
     * Use this in production code to obtain the BuildInfo already parsed when this class was first loaded.
     * Its properties will not change at runtime.
     */
    public static final BuildInfo BUILD_INFO;

    public static final String HAZELCAST_INTERNAL_OVERRIDE_VERSION = "hazelcast.internal.override.version";

    private static final ILogger LOGGER;

    static {
        LOGGER = Logger.getLogger(BuildInfoProvider.class);
        BUILD_INFO = getBuildInfo();
    }

    private BuildInfoProvider() {
    }

    /**
     * Parses {@code hazelcast-runtime.properties} for {@code BuildInfo}; also checks for overrides in System.properties.
     * Use this method to obtain and cache a {@code BuildInfo} object or from test code that needs to re-parse properties
     * on each invocation.
     *
     * @return the parsed BuildInfo
     */
    public static BuildInfo getBuildInfo() {
        Properties properties = loadPropertiesFromResource("hazelcast-runtime.properties");
        Properties enterpriseProperties = loadPropertiesFromResource("hazelcast-enterprise-runtime.properties");
        Properties jetProperties = loadPropertiesFromResource("jet-runtime.properties");

        BuildInfo buildInfo = readBuildInfoProperties(properties, null);
        if (!enterpriseProperties.isEmpty()) {
            buildInfo = readBuildInfoProperties(enterpriseProperties, buildInfo);
        }
        setJetProperties(jetProperties, buildInfo);
        return buildInfo;
    }

    static void setJetProperties(Properties properties, BuildInfo buildInfo) {
        if (properties.isEmpty()) {
            return;
        }
        String version = properties.getProperty("jet.version");
        String build = properties.getProperty("jet.build");
        String revision = properties.getProperty("jet.git.revision");

        JetBuildInfo jetBuildInfo = new JetBuildInfo(version, build, revision);
        buildInfo.setJetBuildInfo(jetBuildInfo);
    }

    private static Properties loadPropertiesFromResource(String resourceName) {
        InputStream properties = BuildInfoProvider.class.getClassLoader().getResourceAsStream(resourceName);
        Properties runtimeProperties = new Properties();
        try {
            if (properties != null) {
                runtimeProperties.load(properties);
            }
        } catch (Exception ignored) {
            ignore(ignored);
        } finally {
            closeResource(properties);
        }
        return runtimeProperties;
    }

    private static BuildInfo readBuildInfoProperties(Properties runtimeProperties, BuildInfo upstreamBuildInfo) {
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
        // override version with a system property
        String overridingVersion = System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        if (overridingVersion != null) {
            LOGGER.info("Overriding hazelcast version with system property value " + overridingVersion);
            version = overridingVersion;
        }

        String sv = runtimeProperties.getProperty("hazelcast.serialization.version");
        byte serialVersion = Byte.parseByte(sv);
        return new BuildInfo(version, build, revision, buildNumber, enterprise, serialVersion, upstreamBuildInfo);
    }
}
