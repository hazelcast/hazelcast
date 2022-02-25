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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.impl.Node;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.lang.Math.min;

/**
 * Collects metadata about this instance
 */
class BuildInfoCollector implements MetricsCollector {

    static final int CLASSPATH_MAX_LENGTH = 100_000;

    private static final String PARDOT_ID_ENV_VAR = "HZ_PARDOT_ID";

    private final Map<String, String> envVars;

    BuildInfoCollector(Map<String, String> envVars) {
        this.envVars = envVars;
    }

    static String formatClassPath(String classpath) {
        String[] classPathEntries = classpath.split(File.pathSeparator);
        String shortenedEntries = Arrays.stream(classPathEntries)
                .filter(cpEntry -> cpEntry.endsWith(".jar"))
                .map(cpEntry -> cpEntry.substring(cpEntry.lastIndexOf(File.separator) + 1))
                .collect(Collectors.joining(","));
        return shortenedEntries.substring(0, min(CLASSPATH_MAX_LENGTH, shortenedEntries.length()));
    }

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        BuildInfo imdgInfo = node.getBuildInfo();
        metricsConsumer.accept(PhoneHomeMetrics.HAZELCAST_DOWNLOAD_ID, getDownloadId());
        metricsConsumer.accept(PhoneHomeMetrics.JAVA_VERSION_OF_SYSTEM, System.getProperty("java.version"));
        metricsConsumer.accept(PhoneHomeMetrics.BUILD_VERSION, imdgInfo.getVersion());
        String classpath = System.getProperty("java.class.path");
        if (classpath != null) {
            metricsConsumer.accept(PhoneHomeMetrics.JAVA_CLASSPATH,
                    formatClassPath(classpath));
        }
    }

    /**
     * Attempts to return the download ID for this instance or returns
     * {@code source} if unable to find the download ID.
     */
    private String getDownloadId() {
        String passedByEnvVar = envVars.get(PARDOT_ID_ENV_VAR);
        if (passedByEnvVar != null) {
            return passedByEnvVar;
        }
        try (InputStream is = getClass().getClassLoader()
                                        .getResourceAsStream("hazelcast-download.properties")) {
            if (is != null) {
                Properties properties = new Properties();
                properties.load(is);
                return properties.getProperty("hazelcastDownloadId");
            }
        } catch (IOException ignored) {
            ignore(ignored);
        }
        return "source";
    }
}
