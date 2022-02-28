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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.reflect.Field;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

/**
 * Provides information about current Hazelcast build.
 */
public final class BuildInfoProvider {

    public static final String HAZELCAST_INTERNAL_OVERRIDE_VERSION = "hazelcast.internal.override.version";
    public static final String HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE = "hazelcast.internal.override.enterprise";
    private static final String HAZELCAST_INTERNAL_OVERRIDE_BUILD = "hazelcast.build";

    private static final ILogger LOGGER;
    private static final BuildInfo BUILD_INFO_CACHE;

    static {
        LOGGER = Logger.getLogger(BuildInfoProvider.class);
        BUILD_INFO_CACHE = populateBuildInfoCache();
    }

    private BuildInfoProvider() {
    }

    private static BuildInfo populateBuildInfoCache() {
        return getBuildInfoInternalVersion(Overrides.DISABLED);
    }

    /**
     * Parses {@code hazelcast-runtime.properties} for {@code BuildInfo}; also checks for overrides in System.properties.
     * Never cache result of this method in a static context - as it can change due versions overriding - this method
     * already does caching whenever it's possible - i.e. when overrides is disabled.
     *
     * @return the parsed BuildInfo
     */
    public static BuildInfo getBuildInfo() {
        if (Overrides.isEnabled()) {
            // never use cache when override is enabled -> we need to re-parse everything
            Overrides overrides = Overrides.fromProperties();
            return getBuildInfoInternalVersion(overrides);
        }

        return BUILD_INFO_CACHE;
    }

    private static BuildInfo getBuildInfoInternalVersion(Overrides overrides) {
        // If you have a compilation error at GeneratedBuildProperties then run 'mvn clean install'
        // the GeneratedBuildProperties class is generated at a compile-time
        BuildInfo buildInfo = readBuildPropertiesClass(GeneratedBuildProperties.class, null, overrides);
        try {
            Class<?> enterpriseClass = BuildInfoProvider.class.getClassLoader()
                    .loadClass("com.hazelcast.instance.GeneratedEnterpriseBuildProperties");
            if (enterpriseClass.getClassLoader() == BuildInfoProvider.class.getClassLoader()) {
                //only read the enterprise properties if there were loaded by the same classloader
                //as BuildInfoProvider and not e.g. a parent classloader.
                buildInfo = readBuildPropertiesClass(enterpriseClass, buildInfo, overrides);
            }
        } catch (ClassNotFoundException e) {
            ignore(e);
        }

        return buildInfo;
    }

    private static BuildInfo readBuildPropertiesClass(Class<?> clazz, BuildInfo upstreamBuildInfo, Overrides overrides) {
        String version = readStaticStringField(clazz, "VERSION");
        String build = readStaticStringField(clazz, "BUILD");
        String revision = readStaticStringField(clazz, "REVISION");
        String distribution = readStaticStringField(clazz, "DISTRIBUTION");
        String commitId = readStaticStringField(clazz, "COMMIT_ID");

        revision = checkMissingExpressionValue(revision, "${git.commit.id.abbrev}");
        commitId = checkMissingExpressionValue(commitId, "${git.commit.id}");

        int buildNumber = Integer.parseInt(build);
        boolean enterprise = !"Hazelcast".equals(distribution);

        String serialVersionString = readStaticStringField(clazz, "SERIALIZATION_VERSION");
        byte serialVersion = Byte.parseByte(serialVersionString);
        return overrides.apply(version, build, revision, buildNumber, enterprise, serialVersion, commitId, upstreamBuildInfo);
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

    private static String checkMissingExpressionValue(String value, String expression) {
        if (!value.isEmpty() && value.equals(expression)) {
            return "";
        } else {
            return value;
        }
    }

    private static final class Overrides {
        private static final Overrides DISABLED = new Overrides(null, -1, null);

        private String version;
        private int buildNo;
        private Boolean enterprise;

        private Overrides(String version, int build, Boolean enterprise) {
            this.version = version;
            this.buildNo = build;
            this.enterprise = enterprise;
        }

        private BuildInfo apply(String version, String build, String revision, int buildNumber,
                                boolean enterprise, byte serialVersion, String commitId, BuildInfo upstreamBuildInfo) {
            if (buildNo != -1) {
                build = String.valueOf(buildNo);
                buildNumber = buildNo;
            }
            if (this.version != null) {
                LOGGER.info("Overriding hazelcast version with system property value " + this.version);
                version = this.version;
            }
            if (this.enterprise != null) {
                LOGGER.info("Overriding hazelcast enterprise flag with system property value " + this.enterprise);
                enterprise = this.enterprise;
            }
            return new BuildInfo(version, build, revision, buildNumber, enterprise, serialVersion,
                    commitId, upstreamBuildInfo);

        }

        private static boolean isEnabled() {
            return System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_BUILD) != null
                    || System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION) != null
                    || System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE) != null;
        }

        private static Overrides fromProperties() {
            String version = System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
            int build = Integer.getInteger(HAZELCAST_INTERNAL_OVERRIDE_BUILD, -1);
            Boolean enterprise = null;
            String enterpriseOverride = System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE);
            if (enterpriseOverride != null) {
                enterprise = Boolean.valueOf(enterpriseOverride);
            }

            return new Overrides(version, build, enterprise);
        }
    }

}
