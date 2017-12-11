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

package com.hazelcast.util;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.ClientType;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.management.ManagementCenterConnectionFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.properties.GroupProperty;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static java.lang.System.getenv;

/**
 * Pings phone home server with cluster info daily.
 */
public final class PhoneHome {

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

    private static final String BASE_PHONE_HOME_URL = "http://phonehome.hazelcast.com/ping";
    private static final int CONNECTION_TIMEOUT_MILLIS = 3000;
    private static final String FALSE = "false";

    public PhoneHome() {
    }

    public void check(final Node hazelcastNode, final String version, final boolean isEnterprise) {
        ILogger logger = hazelcastNode.getLogger(PhoneHome.class);
        if (!hazelcastNode.getProperties().getBoolean(GroupProperty.VERSION_CHECK_ENABLED)) {
            logger.warning(GroupProperty.VERSION_CHECK_ENABLED.getName() + " property is deprecated. Please use "
                    + GroupProperty.PHONE_HOME_ENABLED.getName() + " instead to disable phone home.");
            return;
        }
        if (!hazelcastNode.getProperties().getBoolean(GroupProperty.PHONE_HOME_ENABLED)) {
            return;
        }
        if (FALSE.equals(getenv("HZ_PHONE_HOME_ENABLED"))) {
            return;
        }
        try {
            hazelcastNode.nodeEngine.getExecutionService().scheduleWithRepetition(new Runnable() {
                public void run() {
                    phoneHome(hazelcastNode, version, isEnterprise);
                }
            }, 0, 1, TimeUnit.DAYS);
        } catch (RejectedExecutionException e) {
            logger.warning("Could not schedule phone home! Most probably Hazelcast is failed to start.");
        }
    }

    public void shutdown() {
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

    public Map<String, String> phoneHome(Node hazelcastNode, String version, boolean isEnterprise) {

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
        final ClusterServiceImpl clusterService = hazelcastNode.getClusterService();
        long totalNativeMemorySize = clusterService.getSize(DATA_MEMBER_SELECTOR)
                * memoryConfig.getSize().bytes();
        String nativeMemoryParameter = (isEnterprise)
                ? Long.toString(MemoryUnit.BYTES.toGigaBytes(totalNativeMemorySize)) : "0";

        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        Long clusterUpTime = clusterService.getClusterClock().getClusterUpTime();
        int clusterSize = clusterService.getMembers().size();
        PhoneHomeParameterCreator parameterCreator = new PhoneHomeParameterCreator();
        parameterCreator.addParam("version", version);
        parameterCreator.addParam("m", hazelcastNode.getThisUuid());
        parameterCreator.addParam("e", Boolean.toString(isEnterprise));
        String licenseKey = hazelcastNode.getConfig().getLicenseKey();
        parameterCreator.addParam("l", licenseKey == null ? "" : MD5Util.toMD5String(licenseKey));
        parameterCreator.addParam("p", downloadId);
        parameterCreator.addParam("c", clusterService.getClusterId());
        parameterCreator.addParam("crsz", convertToLetter(clusterSize));
        parameterCreator.addParam("cssz", convertToLetter(hazelcastNode.clientEngine.getClientEndpointCount()));
        parameterCreator.addParam("hdgb", nativeMemoryParameter);
        parameterCreator.addParam("cuptm", Long.toString(clusterUpTime));
        parameterCreator.addParam("nuptm", Long.toString(runtimeMxBean.getUptime()));
        parameterCreator.addParam("jvmn", runtimeMxBean.getVmName());
        parameterCreator.addParam("jvmv", System.getProperty("java.version"));
        JetBuildInfo jetBuildInfo = hazelcastNode.getBuildInfo().getJetBuildInfo();
        parameterCreator.addParam("jetv", jetBuildInfo == null ? "" : jetBuildInfo.getVersion());
        addClientInfo(hazelcastNode, parameterCreator);
        addOSInfo(parameterCreator);

        boolean isManagementCenterConfigEnabled = hazelcastNode.config.getManagementCenterConfig().isEnabled();
        if (isManagementCenterConfigEnabled) {
            addManCenterInfo(hazelcastNode, clusterSize, parameterCreator);
        } else {
            parameterCreator.addParam("mclicense", "MC_NOT_CONFIGURED");
            parameterCreator.addParam("mcver", "MC_NOT_CONFIGURED");
        }

        String urlStr = BASE_PHONE_HOME_URL + parameterCreator.build();
        fetchWebService(urlStr);

        return parameterCreator.getParameters();
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
        } catch (IOException ignored) {
            EmptyStatement.ignore(ignored);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    private void addOSInfo(PhoneHomeParameterCreator parameterCreator) {
        OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
        try {
            parameterCreator.addParam("osn", osMxBean.getName());
            parameterCreator.addParam("osa", osMxBean.getArch());
            parameterCreator.addParam("osv", osMxBean.getVersion());
        } catch (SecurityException e) {
            parameterCreator.addParam("osn", "N/A");
            parameterCreator.addParam("osa", "N/A");
            parameterCreator.addParam("osv", "N/A");
        }
    }

    private void addClientInfo(Node hazelcastNode, PhoneHomeParameterCreator parameterCreator) {
        Map<ClientType, Integer> clusterClientStats = hazelcastNode.clientEngine.getConnectedClientStats();

        parameterCreator.addParam("ccpp", Integer.toString(clusterClientStats.get(ClientType.CPP)));
        parameterCreator.addParam("cdn", Integer.toString(clusterClientStats.get(ClientType.CSHARP)));
        parameterCreator.addParam("cjv", Integer.toString(clusterClientStats.get(ClientType.JAVA)));
        parameterCreator.addParam("cnjs", Integer.toString(clusterClientStats.get(ClientType.NODEJS)));
        parameterCreator.addParam("cpy", Integer.toString(clusterClientStats.get(ClientType.PYTHON)));
    }

    private void addManCenterInfo(Node hazelcastNode, int clusterSize, PhoneHomeParameterCreator parameterCreator) {
        InputStreamReader reader = null;
        InputStream inputStream = null;
        int responseCode;
        String version;
        String license;
        try {
            ManagementCenterConfig managementCenterConfig = hazelcastNode.config.getManagementCenterConfig();
            String manCenterURL = managementCenterConfig.getUrl();
            manCenterURL = manCenterURL.endsWith("/") ? manCenterURL : manCenterURL + '/';
            URL manCenterPhoneHomeURL = new URL(manCenterURL + "phoneHome.do");
            ManagementCenterConnectionFactory connectionFactory
                    = hazelcastNode.getNodeExtension().getManagementCenterConnectionFactory();
            HttpURLConnection connection;
            if (connectionFactory != null) {
                connectionFactory.init(managementCenterConfig.getMutualAuthConfig());
                connection = (HttpURLConnection) connectionFactory.openConnection(manCenterPhoneHomeURL);
            } else {
                connection = (HttpURLConnection) manCenterPhoneHomeURL.openConnection();
            }
            connection.setConnectTimeout(CONNECTION_TIMEOUT_MILLIS);
            connection.setReadTimeout(CONNECTION_TIMEOUT_MILLIS);
            // if management center is not running,
            // connection.getInputStream() throws 'java.net.ConnectException: Connection refused'
            inputStream = connection.getInputStream();
            responseCode = connection.getResponseCode();
            reader = new InputStreamReader(inputStream, "UTF-8");
            JsonObject mcPhoneHomeInfoJson = JsonValue.readFrom(reader).asObject();
            version = JsonUtil.getString(mcPhoneHomeInfoJson, "mcVersion");
            license = JsonUtil.getString(mcPhoneHomeInfoJson, "mcLicense", null);
        } catch (Exception ignored) {
            EmptyStatement.ignore(ignored);
            parameterCreator.addParam("mclicense", "MC_NOT_AVAILABLE");
            parameterCreator.addParam("mcver", "MC_NOT_AVAILABLE");
            return;
        } finally {
            IOUtil.closeResource(reader);
            IOUtil.closeResource(inputStream);
        }

        if (responseCode == HttpURLConnection.HTTP_OK) {
            if (license == null) {
                checkClusterSizeAndSetLicense(clusterSize, parameterCreator);
            } else {
                parameterCreator.addParam("mclicense", license);
            }
            parameterCreator.addParam("mcver", version);
        } else {
            parameterCreator.addParam("mclicense", "MC_CONN_ERR_" + responseCode);
            parameterCreator.addParam("mcver", "MC_CONN_ERR_" + responseCode);
        }
    }

    private void checkClusterSizeAndSetLicense(int clusterSize, PhoneHomeParameterCreator parameterCreator) {
        if (clusterSize <= 2) {
            parameterCreator.addParam("mclicense", "MC_LICENSE_NOT_REQUIRED");
        } else {
            parameterCreator.addParam("mclicense", "MC_LICENSE_REQUIRED_BUT_NOT_SET");
        }
    }

    private static class PhoneHomeParameterCreator {

        private final StringBuilder builder;
        private final Map<String, String> parameters = new HashMap<String, String>();
        private boolean hasParameterBefore;

        public PhoneHomeParameterCreator() {
            builder = new StringBuilder();
            builder.append("?");
        }

        public Map<String, String> getParameters() {
            return parameters;
        }

        public PhoneHomeParameterCreator addParam(String key, String value) {
            if (hasParameterBefore) {
                builder.append("&");
            } else {
                hasParameterBefore = true;
            }
            try {
                builder.append(key).append("=").append(URLEncoder.encode(value, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                ExceptionUtil.rethrow(e);
            }
            parameters.put(key, value);
            return this;
        }

        public String build() {
            return builder.toString();
        }
    }
}
