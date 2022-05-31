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

package com.hazelcast.internal.ascii;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.logging.ILogger;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

import static com.hazelcast.instance.EndpointQualifier.REST;
import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_PLAIN_TEXT;
import static com.hazelcast.internal.util.StringUtil.bytesToString;
import static com.hazelcast.test.Accessors.getNode;

@SuppressWarnings("SameParameterValue")
public class HTTPCommunicator {

    public static final String URI_MAPS = "maps/";
    public static final String URI_QUEUES = "queues/";

    // Instance
    public static final String URI_INSTANCE = "instance";

    // Cluster
    public static final String URI_CLUSTER = "cluster";
    public static final String URI_CLUSTER_STATE_URL = "management/cluster/state";
    public static final String URI_CHANGE_CLUSTER_STATE_URL = "management/cluster/changeState";
    public static final String URI_CLUSTER_VERSION_URL = "management/cluster/version";
    public static final String URI_SHUTDOWN_CLUSTER_URL = "management/cluster/clusterShutdown";
    public static final String URI_SHUTDOWN_NODE_CLUSTER_URL = "management/cluster/memberShutdown";
    public static final String URI_CLUSTER_NODES_URL = "management/cluster/nodes";

    // Hot restart
    public static final String URI_FORCESTART_CLUSTER_URL = "management/cluster/forceStart";
    public static final String URI_PARTIALSTART_CLUSTER_URL = "management/cluster/partialStart";
    public static final String URI_HOT_RESTART_BACKUP_CLUSTER_URL = "management/cluster/hotBackup";
    public static final String URI_HOT_RESTART_BACKUP_INTERRUPT_CLUSTER_URL = "management/cluster/hotBackupInterrupt";

    // WAN
    public static final String URI_WAN_SYNC_MAP = "wan/sync/map";
    public static final String URI_WAN_SYNC_ALL_MAPS = "wan/sync/allmaps";
    public static final String URI_WAN_CLEAR_QUEUES = "wan/clearWanQueues";
    public static final String URI_ADD_WAN_CONFIG = "wan/addWanConfig";
    public static final String URI_WAN_PAUSE_PUBLISHER = "wan/pausePublisher";
    public static final String URI_WAN_STOP_PUBLISHER = "wan/stopPublisher";
    public static final String URI_WAN_RESUME_PUBLISHER = "wan/resumePublisher";
    public static final String URI_WAN_CONSISTENCY_CHECK_MAP = "wan/consistencyCheck/map";

    // License info
    public static final String URI_LICENSE_INFO = "license";

    // CP Subsystem
    public static final String URI_RESET_CP_SUBSYSTEM_URL = "cp-subsystem/reset";
    public static final String URI_CP_GROUPS_URL = "cp-subsystem/groups";
    public static final String URI_CP_SESSIONS_SUFFIX = "/sessions";
    public static final String URI_REMOVE_SUFFIX = "/remove";
    public static final String URI_CP_MEMBERS_URL = "cp-subsystem/members";
    public static final String URI_LOCAL_CP_MEMBER_URL = URI_CP_MEMBERS_URL + "/local";

    // Log Level
    public static final String URI_LOG_LEVEL = "log-level";
    public static final String URI_LOG_LEVEL_RESET = "log-level/reset";

    // Config
    public static final String URI_CONFIG_RELOAD = "config/reload";
    public static final String URI_CONFIG_UPDATE = "config/update";

    private final String address;
    private final boolean sslEnabled;
    private boolean enableChunkedStreaming;
    private final String baseRestAddress;
    private TrustManager[] clientTrustManagers;
    private KeyManager[] clientKeyManagers;
    private String tlsProtocol = "TLSv1.1";
    private final ILogger logger;

    public HTTPCommunicator(HazelcastInstance instance) {
        this(instance, null);
    }

    public HTTPCommunicator(HazelcastInstance instance, String baseRestAddress) {
        logger = instance.getLoggingService().getLogger(HTTPCommunicator.class);
        AdvancedNetworkConfig anc = instance.getConfig().getAdvancedNetworkConfig();
        SSLConfig sslConfig;
        if (anc.isEnabled()) {
            sslConfig = anc.getRestEndpointConfig().getSSLConfig();
        } else {
            sslConfig = instance.getConfig().getNetworkConfig().getSSLConfig();
        }

        sslEnabled = sslConfig != null && sslConfig.isEnabled();
        String protocol = sslEnabled ? "https:/" : "http:/";
        if (baseRestAddress == null) {
            MemberImpl localMember = getNode(instance).getClusterService().getLocalMember();
            this.baseRestAddress = localMember.getSocketAddress(REST).toString();
        } else {
            this.baseRestAddress = baseRestAddress;
        }
        this.address = protocol + this.baseRestAddress + "/hazelcast/rest/";
    }

    HTTPCommunicator(int port) {
        this.baseRestAddress = "/127.0.0.1:" + port;
        this.address = "http:/" + this.baseRestAddress + "/hazelcast/rest/";
        this.sslEnabled = false;
        this.logger = null;
    }

    public String getUrl(String suffix) {
        return address + suffix;
    }

    public HTTPCommunicator setTlsProtocol(String tlsProtocol) {
        this.tlsProtocol = tlsProtocol;
        return this;
    }

    public HTTPCommunicator setClientTrustManagers(TrustManagerFactory factory) {
        this.clientTrustManagers = factory == null ? null : factory.getTrustManagers();
        return this;
    }

    public HTTPCommunicator setClientTrustManagers(TrustManager... clientTrustManagers) {
        this.clientTrustManagers = clientTrustManagers;
        return this;
    }

    public HTTPCommunicator setClientKeyManagers(KeyManager... clientKeyManagers) {
        this.clientKeyManagers = clientKeyManagers;
        return this;
    }

    public HTTPCommunicator setClientKeyManagers(KeyManagerFactory factory) {
        this.clientKeyManagers = factory == null ? null : factory.getKeyManagers();
        return this;
    }

    public String queuePollAndResponse(String queueName, long timeout) throws IOException {
        return queuePoll(queueName, timeout).response;
    }

    public ConnectionResponse queuePoll(String queueName, long timeout) throws IOException {
        String url = getUrl(URI_QUEUES + queueName + "/" + timeout);
        return doGet(url);
    }

    public int queueSize(String queueName) throws IOException {
        String url = getUrl(URI_QUEUES + queueName + "/size");
        return Integer.parseInt(doGet(url).response);
    }

    public int queueOffer(String queueName, String data) throws IOException {
        final String url = getUrl(URI_QUEUES + queueName);
        return doPost(url, data).responseCode;
    }

    public String mapGetAndResponse(String mapName, String key) throws IOException {
        return mapGet(mapName, key).response;
    }

    public ConnectionResponse mapGet(String mapName, String key) throws IOException {
        String url = getUrl(URI_MAPS + mapName + "/" + key);
        return doGet(url);
    }

    public int getHealthReadyResponseCode() throws IOException {
        String url = "http:/" + baseRestAddress + HttpCommandProcessor.URI_HEALTH_READY;
        return doGet(url).responseCode;
    }

    public String getClusterInfo() throws IOException {
        String url = getUrl("cluster");
        return doGet(url).response;
    }

    public String getInstanceInfo() throws IOException {
        String url = getUrl(URI_INSTANCE);
        return doGet(url).response;
    }

    public String getLicenseInfo() throws IOException {
        String url = getUrl(URI_LICENSE_INFO);
        return doGet(url).response;
    }

    public ConnectionResponse setLicense(String clusterName, String clusterPassword, String licenseKey) throws IOException {
        String url = getUrl(URI_LICENSE_INFO);
        return doPost(url, clusterName, clusterPassword, licenseKey);
    }

    public int getFailingClusterHealthWithTrailingGarbage() throws IOException {
        String url = "http:/" + baseRestAddress + HttpCommandProcessor.URI_HEALTH_URL + "garbage";
        return doGet(url).responseCode;
    }

    public String getClusterHealth() throws IOException {
        return getClusterHealth("");
    }

    public String getClusterHealth(String pathParam) throws IOException {
        String url = "http:/" + baseRestAddress + HttpCommandProcessor.URI_HEALTH_URL + pathParam;
        return doGet(url).response;
    }

    public int getClusterHealthResponseCode(String pathParam) throws IOException {
        String url = "http:/" + baseRestAddress + HttpCommandProcessor.URI_HEALTH_URL + pathParam;
        return doGet(url).responseCode;
    }

    public int mapPut(String mapName, String key, String value) throws IOException {
        String url = getUrl(URI_MAPS + mapName + "/" + key);
        return doPost(url, value).responseCode;
    }

    public int mapDeleteAll(String mapName) throws IOException {
        String url = getUrl(URI_MAPS + mapName);
        return doDelete(url).responseCode;
    }

    public int mapDelete(String mapName, String key) throws IOException {
        String url = getUrl(URI_MAPS + mapName + "/" + key);
        return doDelete(url).responseCode;
    }

    public ConnectionResponse shutdownCluster(String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_SHUTDOWN_CLUSTER_URL);
        return doPost(url, clusterName, clusterPassword);
    }

    public ConnectionResponse shutdownMember(String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_SHUTDOWN_NODE_CLUSTER_URL);
        return doPost(url, clusterName, clusterPassword);
    }

    public ConnectionResponse getClusterState(String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_CLUSTER_STATE_URL);
        return doPost(url, clusterName, clusterPassword);
    }

    public ConnectionResponse changeClusterState(String clusterName, String clusterPassword, String newState) throws IOException {
        String url = getUrl(URI_CHANGE_CLUSTER_STATE_URL);
        return doPost(url, clusterName, clusterPassword, newState);
    }

    public String getClusterVersion() throws IOException {
        String url = getUrl(URI_CLUSTER_VERSION_URL);
        return doGet(url).response;
    }

    public ConnectionResponse changeClusterVersion(String clusterName, String clusterPassword, String version) throws IOException {
        String url = getUrl(URI_CLUSTER_VERSION_URL);
        return doPost(url, clusterName, clusterPassword, version);
    }

    public ConnectionResponse hotBackup(String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_HOT_RESTART_BACKUP_CLUSTER_URL);
        return doPost(url, clusterName, clusterPassword);
    }

    public ConnectionResponse hotBackupInterrupt(String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_HOT_RESTART_BACKUP_INTERRUPT_CLUSTER_URL);
        return doPost(url, clusterName, clusterPassword);
    }

    public ConnectionResponse forceStart(String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_FORCESTART_CLUSTER_URL);
        return doPost(url, clusterName, clusterPassword);
    }

    public ConnectionResponse partialStart(String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_PARTIALSTART_CLUSTER_URL);
        return doPost(url, clusterName, clusterPassword);
    }

    public ConnectionResponse listClusterNodes(String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_CLUSTER_NODES_URL);
        return doPost(url, clusterName, clusterPassword);
    }

    public String syncMapOverWAN(String clusterName, String clusterPassword,
                                 String wanRepName, String publisherId, String mapName) throws IOException {
        String url = getUrl(URI_WAN_SYNC_MAP);
        return doPost(url, clusterName, clusterPassword, wanRepName, publisherId, mapName).response;
    }

    public String syncMapsOverWAN(String clusterName, String clusterPassword,
                                  String wanRepName, String publisherId) throws IOException {
        String url = getUrl(URI_WAN_SYNC_ALL_MAPS);
        return doPost(url, clusterName, clusterPassword, wanRepName, publisherId).response;
    }

    public String wanMapConsistencyCheck(String clusterName, String clusterPassword,
                                         String wanRepName, String publisherId, String mapName) throws IOException {
        String url = getUrl(URI_WAN_CONSISTENCY_CHECK_MAP);
        return doPost(url, clusterName, clusterPassword, wanRepName, publisherId, mapName).response;
    }

    public String wanPausePublisher(String clusterName, String clusterPassword,
                                    String wanRepName, String publisherId) throws IOException {
        String url = getUrl(URI_WAN_PAUSE_PUBLISHER);
        return doPost(url, clusterName, clusterPassword, wanRepName, publisherId).response;
    }

    public String wanStopPublisher(String clusterName, String clusterPassword,
                                   String wanRepName, String publisherId) throws IOException {
        String url = getUrl(URI_WAN_STOP_PUBLISHER);
        return doPost(url, clusterName, clusterPassword, wanRepName, publisherId).response;
    }

    public String wanResumePublisher(String clusterName, String clusterPassword,
                                     String wanRepName, String publisherId) throws IOException {
        String url = getUrl(URI_WAN_RESUME_PUBLISHER);
        return doPost(url, clusterName, clusterPassword, wanRepName, publisherId).response;
    }

    public String wanClearQueues(String clusterName, String clusterPassword,
                                 String wanRepName, String targetClusterName) throws IOException {
        String url = getUrl(URI_WAN_CLEAR_QUEUES);
        return doPost(url, clusterName, clusterPassword, wanRepName, targetClusterName).response;
    }

    public String addWanConfig(String clusterName, String clusterPassword,
                               String wanRepConfigJson) throws IOException {
        String url = getUrl(URI_ADD_WAN_CONFIG);
        return doPost(url, clusterName, clusterPassword, wanRepConfigJson).response;
    }

    public ConnectionResponse configReload(String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_CONFIG_RELOAD);
        return doPost(url, clusterName, clusterPassword);
    }

    public ConnectionResponse configUpdate(String clusterName, String clusterPassword, String configAsString) throws IOException {
        String url = getUrl(URI_CONFIG_UPDATE);
        return doPost(url, clusterName, clusterPassword, configAsString);
    }

    public ConnectionResponse getCPGroupIds() throws IOException {
        String url = getUrl(URI_CP_GROUPS_URL);
        return doGet(url);
    }

    public ConnectionResponse getCPGroupByName(String name) throws IOException {
        String url = getUrl(URI_CP_GROUPS_URL + "/" + name);
        return doGet(url);
    }

    public ConnectionResponse getLocalCPMember() throws IOException {
        String url = getUrl(URI_LOCAL_CP_MEMBER_URL);
        return doGet(url);
    }

    public ConnectionResponse getCPMembers() throws IOException {
        String url = getUrl(URI_CP_MEMBERS_URL);
        return doGet(url);
    }

    public ConnectionResponse forceDestroyCPGroup(String cpGroupName, String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_CP_GROUPS_URL + "/" + cpGroupName + URI_REMOVE_SUFFIX);
        return doPost(url, clusterName, clusterPassword);
    }

    public ConnectionResponse removeCPMember(UUID cpMemberUid, String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_CP_MEMBERS_URL + "/" + cpMemberUid + URI_REMOVE_SUFFIX);
        return doPost(url, clusterName, clusterPassword);
    }

    public ConnectionResponse promoteCPMember(String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_CP_MEMBERS_URL);
        return doPost(url, clusterName, clusterPassword);
    }

    public ConnectionResponse restart(String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_RESET_CP_SUBSYSTEM_URL);
        return doPost(url, clusterName, clusterPassword);
    }

    public ConnectionResponse getCPSessions(String clusterName) throws IOException {
        String url = getUrl(URI_CP_GROUPS_URL + "/" + clusterName + URI_CP_SESSIONS_SUFFIX);
        return doGet(url);
    }

    public ConnectionResponse forceCloseCPSession(String cpGroupName, long sessionId, String clusterName, String clusterPassword)
            throws IOException {
        String url = getUrl(URI_CP_GROUPS_URL + "/" + cpGroupName + URI_CP_SESSIONS_SUFFIX + "/" + sessionId + URI_REMOVE_SUFFIX);
        return doPost(url, clusterName, clusterPassword);
    }

    public static class ConnectionResponse {
        public final String response;
        public final int responseCode;
        public final Map<String, List<String>> responseHeaders;

        ConnectionResponse(CloseableHttpResponse httpResponse) throws IOException {
            int responseCode = httpResponse.getStatusLine().getStatusCode();
            HttpEntity entity = httpResponse.getEntity();
            String responseStr = entity != null ? EntityUtils.toString(entity, "UTF-8") : "";
            Header[] headers = httpResponse.getAllHeaders();
            Map<String, List<String>> responseHeaders = new HashMap<>();
            for (Header header : headers) {
                List<String> values = responseHeaders.get(header.getName());
                if (values == null) {
                    values = new ArrayList<>();
                    responseHeaders.put(header.getName(), values);
                }
                values.add(header.getValue());
            }
            this.responseCode = responseCode;
            this.response = responseStr;
            this.responseHeaders = responseHeaders;
        }

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder("HTTP ").append(responseCode).append("\r\n");
            responseHeaders.forEach((name, values) -> {
                values.forEach(headerValue -> str.append(name).append(": ").append(headerValue).append("\r\n"));
            });
            str.append("\r\n");
            return str.append(response).toString();
        }
    }

    private ConnectionResponse doHead(String url) throws IOException {
        logRequest("HEAD", url);
        CloseableHttpClient client = newClient();
        CloseableHttpResponse response = null;
        try {
            HttpHead request = new HttpHead(url);
            response = client.execute(request);
            return new ConnectionResponse(response);
        } finally {
            IOUtil.closeResource(response);
            IOUtil.closeResource(client);
        }
    }

    public ConnectionResponse doGet(String url) throws IOException {
        logRequest("GET", url);
        CloseableHttpClient client = newClient();
        CloseableHttpResponse response = null;
        try {
            HttpGet request = new HttpGet(url);
            request.setHeader("Content-type", "text/xml; charset=" + "UTF-8");
            response = client.execute(request);
            return new ConnectionResponse(response);
        } finally {
            IOUtil.closeResource(response);
            IOUtil.closeResource(client);
        }
    }

    public ConnectionResponse doPost(String url, String... params) throws IOException {
        logRequest("POST", url);
        CloseableHttpClient client = newClient();

        List<NameValuePair> nameValuePairs = new ArrayList<>(params.length);
        for (String param : params) {
            nameValuePairs.add(new BasicNameValuePair(param == null ? "" : param, null));
        }
        String data = URLEncodedUtils.format(nameValuePairs, Consts.UTF_8);

        HttpEntity entity;
        ContentType contentType = ContentType.create(bytesToString(CONTENT_TYPE_PLAIN_TEXT), Consts.UTF_8);
        if (enableChunkedStreaming) {
            ByteArrayInputStream stream = new ByteArrayInputStream(data.getBytes(Consts.UTF_8));
            InputStreamEntity streamEntity = new InputStreamEntity(stream, contentType);
            streamEntity.setChunked(true);
            entity = streamEntity;
        } else {
            entity = new StringEntity(data, contentType);
        }

        CloseableHttpResponse response = null;
        try {
            HttpPost request = new HttpPost(url);
            request.setEntity(entity);
            response = client.execute(request);

            return new ConnectionResponse(response);
        } finally {
            IOUtil.closeResource(response);
            IOUtil.closeResource(client);
        }
    }

    public ConnectionResponse doDelete(String url) throws IOException {
        logRequest("DELETE", url);
        CloseableHttpClient client = newClient();
        CloseableHttpResponse response = null;
        try {
            HttpDelete request = new HttpDelete(url);
            request.setHeader("Content-type", "text/xml; charset=" + "UTF-8");
            response = client.execute(request);
            return new ConnectionResponse(response);
        } finally {
            IOUtil.closeResource(response);
            IOUtil.closeResource(client);
        }
    }

    private void logRequest(String method, String url) {
        if (logger != null && logger.isFineEnabled()) {
            logger.fine("Sending " + method + " request to " + url);
        }
    }

    private CloseableHttpClient newClient() throws IOException {
        HttpClientBuilder builder = HttpClients.custom();

        if (sslEnabled) {
            SSLContext sslContext;
            try {
                sslContext = SSLContext.getInstance(tlsProtocol);
            } catch (NoSuchAlgorithmException e) {
                throw new IOException(e);
            }

            try {
                sslContext.init(clientKeyManagers, clientTrustManagers, new SecureRandom());
            } catch (KeyManagementException e) {
                throw new IOException(e);
            }

            builder.setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext,
                    SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER));
        }

        return builder.build();
    }

    public ConnectionResponse headRequestToMapURI() throws IOException {
        String url = getUrl(URI_MAPS);
        return doHead(url);
    }

    public ConnectionResponse headRequestToQueueURI() throws IOException {
        String url = getUrl(URI_QUEUES);
        return doHead(url);
    }

    public ConnectionResponse headRequestToUndefinedURI() throws IOException {
        String url = getUrl("undefined");
        return doHead(url);
    }

    public ConnectionResponse getRequestToUndefinedURI() throws IOException {
        String url = getUrl("undefined");
        return doGet(url);
    }

    public ConnectionResponse postRequestToUndefinedURI() throws IOException {
        String url = getUrl("undefined");
        return doPost(url);
    }

    public ConnectionResponse deleteRequestToUndefinedURI() throws IOException {
        String url = getUrl("undefined");
        return doDelete(url);
    }

    public ConnectionResponse headRequestToClusterInfoURI() throws IOException {
        String url = getUrl(URI_CLUSTER);
        return doHead(url);
    }

    public ConnectionResponse getBadRequestURI() throws IOException {
        String url = getUrl(URI_MAPS + "name");
        return doGet(url);
    }

    public ConnectionResponse postBadRequestURI() throws IOException {
        String url = address + "maps/name";
        return doPost(url);
    }

    public ConnectionResponse deleteBadRequestURI() throws IOException {
        String url = getUrl(URI_QUEUES + "name");
        return doDelete(url);
    }

    public ConnectionResponse headRequestToClusterHealthURI() throws IOException {
        String url = "http:/" + baseRestAddress + HttpCommandProcessor.URI_HEALTH_URL;
        return doHead(url);
    }

    public ConnectionResponse headRequestToClusterVersionURI() throws IOException {
        String url = getUrl(URI_CLUSTER_VERSION_URL);
        return doHead(url);
    }

    public ConnectionResponse headRequestToGarbageClusterHealthURI() throws IOException {
        String url = "http:/" + baseRestAddress + HttpCommandProcessor.URI_HEALTH_URL + "garbage";
        return doHead(url);
    }

    public ConnectionResponse headRequestToInstanceURI() throws IOException {
        String url = getUrl(URI_INSTANCE);
        return doHead(url);
    }

    public void enableChunkedStreaming() {
        this.enableChunkedStreaming = true;
    }

    public ConnectionResponse getLogLevel() throws IOException {
        String url = getUrl(URI_LOG_LEVEL);
        return doGet(url);
    }

    public ConnectionResponse setLogLevel(String clusterName, String clusterPassword, Level level) throws IOException {
        String url = getUrl(URI_LOG_LEVEL);
        return doPost(url, clusterName, clusterPassword, level.getName());
    }

    public ConnectionResponse resetLogLevel(String clusterName, String clusterPassword) throws IOException {
        String url = getUrl(URI_LOG_LEVEL_RESET);
        return doPost(url, clusterName, clusterPassword);
    }

}
