/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.ClientType;
import com.hazelcast.instance.Node;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.IOUtil;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Checks version of hazelcast with central server.
 */
public final class VersionCheck {

    private static final int TIMEOUT = 1000;
    private static final int A_INTERVAL = 5;
    private static final int B_INTERVAL = 10;
    private static final int C_INTERVAL = 20;
    private static final int D_INTERVAL = 40;
    private static final int E_INTERVAL = 60;
    private static final int F_INTERVAL = 100;
    private static final int G_INTERVAL = 150;
    private static final int H_INTERVAL = 300;
    private static final int J_INTERVAL = 600;

    private static final String BASE_VERSION_CHECK_URL = "http://versioncheck.hazelcast.com/version.jsp";

    public VersionCheck() {
    }

    public void check(final Node hazelcastNode, final String version, final boolean isEnterprise) {
        if (!hazelcastNode.getGroupProperties().VERSION_CHECK_ENABLED.getBoolean()) {
            return;
        }
        hazelcastNode.nodeEngine.getExecutionService().scheduleAtFixedRate(new Runnable() {
            public void run() {
                doCheck(hazelcastNode, version, isEnterprise);
            }
        }, 0, 1, TimeUnit.DAYS);
    }

    public void shutdown() {
    }

    private String convertToLetter(int size) {
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

    private void doCheck(Node hazelcastNode, String version, boolean isEnterprise) {
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
            EmptyStatement.ignore(ignored);
        } finally {
            IOUtil.closeResource(is);
        }

        //Calculate native memory usage from native memory config
        NativeMemoryConfig memoryConfig = hazelcastNode.getConfig().getNativeMemoryConfig();
        long totalNativeMemorySize = hazelcastNode.getClusterService().getSize()
                * memoryConfig.getSize().bytes();
        String nativeMemoryParameter = (isEnterprise)
                ? Long.toString(MemoryUnit.BYTES.toGigaBytes(totalNativeMemorySize)) : "0";
        //Calculate connected clients to the cluster.
        Map<ClientType, Integer> clusterClientStats = hazelcastNode.clientEngine.getConnectedClientStats();

        UrlActionParameterCreator parameterCreator = new UrlActionParameterCreator();
        parameterCreator.addParam("version", version);
        parameterCreator.addParam("m", hazelcastNode.getLocalMember().getUuid());
        parameterCreator.addParam("e", Boolean.toString(isEnterprise));
        parameterCreator.addParam("l", MD5Util.toMD5String(hazelcastNode.getConfig().getLicenseKey()));
        parameterCreator.addParam("p", downloadId);
        parameterCreator.addParam("c", MD5Util.toMD5String(hazelcastNode.getConfig().getGroupConfig().getName()));
        parameterCreator.addParam("crsz", convertToLetter(hazelcastNode.getClusterService().getMembers().size()));
        parameterCreator.addParam("cssz", convertToLetter(hazelcastNode.clientEngine.getClientEndpointCount()));
        parameterCreator.addParam("hdgb", nativeMemoryParameter);
        parameterCreator.addParam("ccpp", Integer.toString(clusterClientStats.get(ClientType.CPP)));
        parameterCreator.addParam("cdn", Integer.toString(clusterClientStats.get(ClientType.CSHARP)));
        parameterCreator.addParam("cjv", Integer.toString(clusterClientStats.get(ClientType.JAVA)));

        String urlStr = BASE_VERSION_CHECK_URL + parameterCreator.build();
        fetchWebService(urlStr);
    }

    private void fetchWebService(String urlStr) {
        InputStream in = null;
        try {
            URL url = new URL(urlStr);
            URLConnection conn = url.openConnection();
            conn.setRequestProperty("User-Agent", "Mozilla/5.0");
            conn.setConnectTimeout(TIMEOUT * 2);
            conn.setReadTimeout(TIMEOUT * 2);
            in = new BufferedInputStream(conn.getInputStream());
        }  catch (IOException ignored) {
            EmptyStatement.ignore(ignored);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    private static class UrlActionParameterCreator {

        private final StringBuilder builder;
        private boolean hasParameterBefore;

        public UrlActionParameterCreator() {
            builder = new StringBuilder();
            builder.append("?");
        }

        public UrlActionParameterCreator addParam(String key, String value) {
            if (hasParameterBefore) {
                builder.append("&");
            } else {
                hasParameterBefore = true;
            }
            builder.append(key).append("=").append(value);
            return this;
        }

        public String build() {
            return builder.toString();
        }
    }
}
