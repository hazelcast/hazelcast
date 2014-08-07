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

/**
 * Utility class to get hazelcast version , build number from properties file.
 */
public final class HazelcastUtil {

    private static  String VERSION;
    private static  boolean ENTERPRISE;
    private static  String BUILD;
    private static  int BUILD_NUMBER;

    //CHECKSTYLE:OFF
    static {
        init();
    }

    //CHECKSTYLE:ON

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
        return ENTERPRISE;
    }

    public static void init() {
        final InputStream inRuntimeProperties =
                NodeInitializer.class.getClassLoader().getResourceAsStream("hazelcast-runtime.properties");
        Properties runtimeProperties = new Properties();
        try {
            if (inRuntimeProperties != null) {
                runtimeProperties.load(inRuntimeProperties);
                inRuntimeProperties.close();
            }
        } catch (Exception ignored) {
            EmptyStatement.ignore(ignored);
        }

        VERSION = runtimeProperties.getProperty("hazelcast.version");
        String distribution = runtimeProperties.getProperty("hazelcast.distribution");
        ENTERPRISE = "Hazelcast".equals(distribution) ? false : true;

        // override BUILD_NUMBER with a system property
        Integer buildNumber = Integer.getInteger("hazelcast.build", -1);
        if (buildNumber == -1) {
            BUILD = runtimeProperties.getProperty("hazelcast.build");
        } else {
            BUILD = String.valueOf(buildNumber);
        }
        BUILD_NUMBER = Integer.valueOf(BUILD);
    }

}
