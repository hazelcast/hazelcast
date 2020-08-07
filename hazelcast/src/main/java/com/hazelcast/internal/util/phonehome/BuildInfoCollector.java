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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.instance.impl.Node;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.EmptyStatement.ignore;

class BuildInfoCollector implements MetricsCollector {

    private String getDownloadId() {
        String downloadId = "source";
        InputStream is = null;
        try {
            is = getClass().getClassLoader().getResourceAsStream("hazelcast-download.properties");
            if (is != null) {
                final Properties properties = new Properties();
                properties.load(is);
                downloadId = properties.getProperty("hazelcastDownloadId");
            }
        } catch (IOException ignored) {
            ignore(ignored);
        } finally {
            closeResource(is);
        }
        return downloadId;
    }

    @Override
    public Map<PhoneHomeMetrics, String> computeMetrics(Node hazelcastNode) {

        BuildInfo build = BuildInfoProvider.getBuildInfo();
        Map<PhoneHomeMetrics, String> buildInfo = new HashMap<>();
        JetBuildInfo jetBuildInfo = hazelcastNode.getBuildInfo().getJetBuildInfo();
        buildInfo.put(PhoneHomeMetrics.HAZELCAST_DOWNLOAD_ID, getDownloadId());
        buildInfo.put(PhoneHomeMetrics.CLIENT_ENDPOINT_COUNT,
                MetricsCollector.convertToLetter(hazelcastNode.clientEngine.getClientEndpointCount()));
        buildInfo.put(PhoneHomeMetrics.JAVA_VERSION_OF_SYSTEM, System.getProperty("java.version"));
        buildInfo.put(PhoneHomeMetrics.BUILD_VERSION, build.getVersion());
        buildInfo.put(PhoneHomeMetrics.JET_BUILD_VERSION, jetBuildInfo == null ? "" : jetBuildInfo.getVersion());

        return buildInfo;
    }
}
