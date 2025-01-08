/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.version.MemberVersion;

import javax.annotation.Nullable;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.jet.impl.util.ReflectionUtils.readStaticField;
import static com.hazelcast.jet.impl.util.ReflectionUtils.readStaticFieldOrNull;
import static java.util.Objects.requireNonNull;

/**
 * Provides information about current Hazelcast build.
 */
public final class BuildInfoProvider {

    public static final String HAZELCAST_INTERNAL_OVERRIDE_VERSION = "hazelcast.internal.override.version";
    public static final String HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE = "hazelcast.internal.override.enterprise";
    public static final String HAZELCAST_INTERNAL_OVERRIDE_LAST_LTS = "hazelcast.internal.override.lastLtsVersion";
    public static final String HAZELCAST_INTERNAL_OVERRIDE_PREV_VERSION = "hazelcast.internal.override.previousVersion";
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
        String version = readStaticField(clazz, "VERSION");
        String build = readStaticField(clazz, "BUILD");
        String revision = readStaticField(clazz, "REVISION");
        String distribution = readStaticFieldOrNull(clazz, "DISTRIBUTION");
        String commitId = readStaticFieldOrNull(clazz, "COMMIT_ID");

        revision = checkMissingExpressionValue(revision, "${git.commit.id.abbrev}");
        commitId = checkMissingExpressionValue(commitId, "${git.commit.id}");
        String lastLtsVersion = readStaticField(clazz, "LAST_LTS_VERSION");
        String previousVersion = readStaticField(clazz, "PREVIOUS_MINOR_VERSION");

        int buildNumber = Integer.parseInt(build);
        boolean enterprise = !"Hazelcast".equals(distribution);

        String serialVersionString = readStaticFieldOrNull(clazz, "SERIALIZATION_VERSION");
        requireNonNull(serialVersionString, "serialization version must not be null");
        byte serialVersion = Byte.parseByte(serialVersionString);
        return overrides.apply(version, build, revision, buildNumber, enterprise, serialVersion, commitId, upstreamBuildInfo,
                MemberVersion.of(lastLtsVersion), lastLtsVersion, MemberVersion.of(previousVersion));
    }

    private static String checkMissingExpressionValue(@Nullable String value, String expression) {
        if (value != null && !value.isEmpty() && value.equals(expression)) {
            return "";
        } else {
            return value;
        }
    }

    private static final class Overrides {
        private static final Overrides DISABLED = new Overrides(null, -1, null, null,  null, null);

        private final String version;
        private final int buildNo;
        private final Boolean enterprise;
        private final MemberVersion lastLtsVersion;
        private final String lastLtsVersionString;
        private final MemberVersion previousVersion;

        private Overrides(String version, int build, Boolean enterprise,
                          MemberVersion lastLtsVersion, String lastLtsVersionString, MemberVersion previousVersion) {
            this.version = version;
            this.buildNo = build;
            this.enterprise = enterprise;
            this.lastLtsVersion = lastLtsVersion;
            this.lastLtsVersionString = lastLtsVersionString;
            this.previousVersion = previousVersion;
        }

        @SuppressWarnings("checkstyle:ParameterNumber")
        private BuildInfo apply(String version, String build, String revision, int buildNumber,
                                boolean enterprise, byte serialVersion, String commitId, BuildInfo upstreamBuildInfo,
                                MemberVersion lastLtsVersion, String lastLtsVersionString, MemberVersion previousVersion) {
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
            if (this.lastLtsVersion != null) {
                LOGGER.info("Overriding hazelcast last LTS version with system property value " + this.lastLtsVersion);
                lastLtsVersion = this.lastLtsVersion;
            }
            if (this.lastLtsVersionString != null) {
                LOGGER.info("Overriding hazelcast last LTS version with system property value " + this.lastLtsVersionString);
                lastLtsVersionString = this.lastLtsVersionString;
            }
            if (this.previousVersion != null) {
                LOGGER.info("Overriding hazelcast previous version with system property value " + this.previousVersion);
                previousVersion = this.previousVersion;
            }
            return new BuildInfo(version, build, revision, buildNumber, enterprise, serialVersion,
                    commitId, upstreamBuildInfo, lastLtsVersion, lastLtsVersionString, previousVersion);

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
            String lastLts = System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_LAST_LTS);
            MemberVersion lastLtsVersion = null;
            if (lastLts != null) {
                lastLtsVersion = MemberVersion.of(lastLts);
            }
            String prev = System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_PREV_VERSION);
            MemberVersion prevVersion = null;
            if (prev != null) {
                prevVersion = MemberVersion.of(prev);
            }

            return new Overrides(version, build, enterprise, lastLtsVersion, lastLts, prevVersion);
        }
    }

}
