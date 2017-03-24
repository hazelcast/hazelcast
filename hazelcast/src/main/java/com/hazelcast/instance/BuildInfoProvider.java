/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.InputStream;
import java.lang.reflect.Field;
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
        // If you have a compilation error at GeneratedBuildProperties then run 'mvn clean install'
        // the GeneratedBuildProperties class is generated at a compile-time
        BuildInfo buildInfo = readBuildPropertiesClass(GeneratedBuildProperties.class, null);
        try {
            Class<?> enterpriseClass = BuildInfoProvider.class.getClassLoader()
                    .loadClass("com.hazelcast.instance.GeneratedEnterpriseBuildProperties");
            if (enterpriseClass.getClassLoader() == BuildInfoProvider.class.getClassLoader()) {
                //only read the enterprise properties if there were loaded by the same classloader
                //as BuildInfoProvider and not e.g. a parent classloader.
                buildInfo = readBuildPropertiesClass(enterpriseClass, buildInfo);
            }
        } catch (ClassNotFoundException e) {
            ignore(e);
        }

        Properties jetProperties = loadPropertiesFromResource("jet-runtime.properties");
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

    private static BuildInfo readBuildPropertiesClass(Class<?> clazz, BuildInfo upstreamBuildInfo) {
        String version = readStaticStringField(clazz, "VERSION");
        String build = readStaticStringField(clazz, "BUILD");
        String revision = readStaticStringField(clazz, "REVISION");
        String distribution = readStaticStringField(clazz, "DISTRIBUTION");

        if (!revision.isEmpty() && revision.equals("${git.commit.id.abbrev}")) {
            revision = "";
        }
        int buildNumber = Integer.parseInt(build);
        boolean enterprise = !"Hazelcast".equals(distribution);

        String serialVersionString = readStaticStringField(clazz, "SERIALIZATION_VERSION");
        byte serialVersion = Byte.parseByte(serialVersionString);
        return overrideBuildInfo(version, build, revision, buildNumber, enterprise, serialVersion, upstreamBuildInfo);
    }

    private static BuildInfo overrideBuildInfo(String version, String build, String revision, int buildNumber,
                                               boolean enterprise, byte serialVersion, BuildInfo upstreamBuildInfo) {
        Integer hazelcastBuild = Integer.getInteger("hazelcast.build", -1);
        if (hazelcastBuild != -1) {
            build = String.valueOf(hazelcastBuild);
            buildNumber = hazelcastBuild;
        }
        String overridingVersion = System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        if (overridingVersion != null) {
            LOGGER.info("Overriding hazelcast version with system property value " + overridingVersion);
            version = overridingVersion;
        }
        return new BuildInfo(version, build, revision, buildNumber, enterprise, serialVersion, upstreamBuildInfo);
    }

    //todo: move elsewhere
    private static String readStaticStringField(Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getField(fieldName);
            return (String) field.get(null);
        } catch (NoSuchFieldException e) {
            throw new HazelcastException(e);
        } catch (IllegalAccessException e) {
            throw new HazelcastException(e);
        }
    }

}
