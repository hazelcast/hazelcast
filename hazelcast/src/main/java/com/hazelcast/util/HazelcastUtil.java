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

package com.hazelcast.util;

import com.hazelcast.instance.NodeInitializer;

import java.io.InputStream;
import java.util.Properties;

public final class HazelcastUtil {

    private static final String VERSION;
    private static final boolean IS_ENTERPRISE;
    private static final String BUILD;
    private static final int BUILD_NUMBER;

    static {
        String version = System.getProperty("hazelcast.version", "unknown");
        String build = System.getProperty("hazelcast.build", "unknown");
        int buildNumber = 0;
        if ("unknown".equals(version) || "unknown".equals(build)) {
            try {
                final InputStream inRuntimeProperties =
                        NodeInitializer.class.getClassLoader().getResourceAsStream("hazelcast-runtime.properties");
                if (inRuntimeProperties != null) {
                    Properties runtimeProperties = new Properties();
                    runtimeProperties.load(inRuntimeProperties);
                    inRuntimeProperties.close();
                    version = runtimeProperties.getProperty("hazelcast.version");
                    build = runtimeProperties.getProperty("hazelcast.build");
                }
            } catch (Exception ignored) {
            }
        }
        try {
            buildNumber = Integer.getInteger("hazelcast.build", -1);
            if (buildNumber == -1) {
                buildNumber = Integer.parseInt(build);
            }
        } catch (Exception ignored) {
        }
        VERSION = version;
        BUILD = build;
        BUILD_NUMBER = buildNumber;
        IS_ENTERPRISE = version.endsWith("-ee");
    }

    private HazelcastUtil() {
    }

    public static String getVersion() {
        return VERSION;
    }

    public static String getBuild() {
        return BUILD;
    }

    public static int getBuildNumber() {
        return BUILD_NUMBER;
    }

    public static boolean isEnterprise() {
        return IS_ENTERPRISE;
    }

}
