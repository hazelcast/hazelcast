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
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.util.phonehome.metrics.MapMetrics;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.EmptyStatement.ignore;

public class MetricsCollectorImpl implements MetricsCollector {

    private static final int A_INTERVAL = 5;
    private static final int B_INTERVAL = 10;
    private static final int C_INTERVAL = 20;
    private static final int D_INTERVAL = 40;
    private static final int E_INTERVAL = 60;
    private static final int F_INTERVAL = 100;
    private static final int G_INTERVAL = 150;
    private static final int H_INTERVAL = 300;
    private static final int J_INTERVAL = 600;

    PhoneHomeParameterCreator parameterCreator;
    private final Node hazelcastNode;

    private final BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();

    public MetricsCollectorImpl(Node node) {
        hazelcastNode = node;
        parameterCreator = new PhoneHomeParameterCreator();
        addBuildInfo();
        addClusterInfo();
        addClientInfo();
        addMapInfo();
        addOSInfo();

    }

    public String convertToLetter(int size) {
        String letter;
        if (size < A_INTERVAL) {
            letter = "A";
        } else if (size < B_INTERVAL) {
            letter = "B";
        } else if (size < C_INTERVAL) {
            letter = "C";
        } else if (size < D_INTERVAL) {
            letter = "D";
        } else if (size < E_INTERVAL) {
            letter = "E";
        } else if (size < F_INTERVAL) {
            letter = "F";
        } else if (size < G_INTERVAL) {
            letter = "G";
        } else if (size < H_INTERVAL) {
            letter = "H";
        } else if (size < J_INTERVAL) {
            letter = "J";
        } else {
            letter = "I";
        }
        return letter;
    }

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
    public void addBuildInfo() {
        JetBuildInfo jetBuildInfo = hazelcastNode.getBuildInfo().getJetBuildInfo();
        parameterCreator
                .addParam("p", getDownloadId())
                .addParam("cssz", convertToLetter(hazelcastNode.clientEngine.getClientEndpointCount()))
                .addParam("jvmv", System.getProperty("java.version"))
                .addParam("version", buildInfo.getVersion())
                .addParam("jetv", jetBuildInfo == null ? "" : jetBuildInfo.getVersion());
    }

    @Override
    public void addClusterInfo() {
        ClusterServiceImpl clusterService = hazelcastNode.getClusterService();
        int clusterSize = clusterService.getMembers().size();
        long clusterUpTime = clusterService.getClusterClock().getClusterUpTime();
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        parameterCreator
                .addParam("m", hazelcastNode.getThisUuid().toString())
                .addParam("c", clusterService.getClusterId().toString())
                .addParam("crsz", convertToLetter(clusterSize))
                .addParam("cuptm", Long.toString(clusterUpTime))
                .addParam("nuptm", Long.toString(runtimeMxBean.getUptime()))
                .addParam("jvmn", runtimeMxBean.getVmName());

    }

    @Override
    public void addOSInfo() {
        OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
        try {
            parameterCreator
                    .addParam("osn", osMxBean.getName())
                    .addParam("osa", osMxBean.getArch())
                    .addParam("osv", osMxBean.getVersion());
        } catch (SecurityException e) {
            parameterCreator
                    .addParam("osn", "N/A")
                    .addParam("osa", "N/A")
                    .addParam("osv", "N/A");
        }

    }

    @Override
    public void addClientInfo() {
        Map<String, Integer> clusterClientStats = hazelcastNode.clientEngine.getConnectedClientStats();
        parameterCreator
                .addParam("ccpp", Integer.toString(clusterClientStats.getOrDefault(ConnectionType.CPP_CLIENT, 0)))
                .addParam("cdn", Integer.toString(clusterClientStats.getOrDefault(ConnectionType.CSHARP_CLIENT, 0)))
                .addParam("cjv", Integer.toString(clusterClientStats.getOrDefault(ConnectionType.JAVA_CLIENT, 0)))
                .addParam("cnjs", Integer.toString(clusterClientStats.getOrDefault(ConnectionType.NODEJS_CLIENT, 0)))
                .addParam("cpy", Integer.toString(clusterClientStats.getOrDefault(ConnectionType.PYTHON_CLIENT, 0)))
                .addParam("cgo", Integer.toString(clusterClientStats.getOrDefault(ConnectionType.GO_CLIENT, 0)));
    }

    @Override
    public void addMapInfo() {
        MapMetrics mapmetrics = new MapMetrics(hazelcastNode);
        parameterCreator.addParam("mpct", String.valueOf(mapmetrics.getMapCount()));

    }
}
