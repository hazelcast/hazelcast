/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.nio.IOUtil;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("SameParameterValue")
public class HTTPCommunicator {

    private final HazelcastInstance instance;
    private final String address;
    private final boolean sslEnabled;
    private boolean enableChunkedStreaming;
    private TrustManager[] clientTrustManagers;
    private KeyManager[] clientKeyManagers;
    private String tlsProtocol = "TLSv1.1";

    public HTTPCommunicator(HazelcastInstance instance) {
        this.instance = instance;

        SSLConfig sslConfig = instance.getConfig().getNetworkConfig().getSSLConfig();
        sslEnabled = sslConfig != null && sslConfig.isEnabled();
        String protocol = sslEnabled ? "https:/" : "http:/";
        this.address = protocol + instance.getCluster().getLocalMember().getSocketAddress().toString() + "/hazelcast/rest/";
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

    public String queuePoll(String queueName, long timeout) throws IOException {
        String url = address + "queues/" + queueName + "/" + String.valueOf(timeout);
        return doGet(url).response;
    }

    public int queueSize(String queueName) throws IOException {
        String url = address + "queues/" + queueName + "/size";
        return Integer.parseInt(doGet(url).response);
    }

    public int queueOffer(String queueName, String data) throws IOException {
        final String url = address + "queues/" + queueName;
        return doPost(url, data).responseCode;
    }

    public String mapGet(String mapName, String key) throws IOException {
        String url = address + "maps/" + mapName + "/" + key;
        return doGet(url).response;
    }

    public String getClusterInfo() throws IOException {
        String url = address + "cluster";
        return doGet(url).response;
    }

    public String getLicenseInfo() throws IOException {
        String url = address + "license";
        return doGet(url).response;
    }

    public int getFailingClusterHealthWithTrailingGarbage() throws IOException {
        String baseAddress = instance.getCluster().getLocalMember().getSocketAddress().toString();
        String url = "http:/" + baseAddress + HttpCommandProcessor.URI_HEALTH_URL + "garbage";
        return doGet(url).responseCode;
    }

    public String getClusterHealth() throws IOException {
        return getClusterHealth("");
    }

    public String getClusterHealth(String pathParam) throws IOException {
        String baseAddress = instance.getCluster().getLocalMember().getSocketAddress().toString();
        String url = "http:/" + baseAddress + HttpCommandProcessor.URI_HEALTH_URL + pathParam;
        return doGet(url).response;
    }

    public int getClusterHealthResponseCode(String pathParam) throws IOException {
        String baseAddress = instance.getCluster().getLocalMember().getSocketAddress().toString();
        String url = "http:/" + baseAddress + HttpCommandProcessor.URI_HEALTH_URL + pathParam;
        return doGet(url).responseCode;
    }

    public int mapPut(String mapName, String key, String value) throws IOException {
        final String url = address + "maps/" + mapName + "/" + key;
        return doPost(url, value).responseCode;
    }

    public int mapDeleteAll(String mapName) throws IOException {
        String url = address + "maps/" + mapName;
        return doDelete(url).responseCode;
    }

    public int mapDelete(String mapName, String key) throws IOException {
        String url = address + "maps/" + mapName + "/" + key;
        return doDelete(url).responseCode;
    }

    public int shutdownCluster(String groupName, String groupPassword) throws IOException {
        String url = address + "management/cluster/clusterShutdown";
        return doPost(url, groupName, groupPassword).responseCode;
    }

    public String shutdownMember(String groupName, String groupPassword) throws IOException {
        String url = address + "management/cluster/memberShutdown";
        return doPost(url, groupName, groupPassword).response;
    }

    public String getClusterState(String groupName, String groupPassword) throws IOException {
        String url = address + "management/cluster/state";
        return doPost(url, groupName, groupPassword).response;
    }

    public ConnectionResponse changeClusterState(String groupName, String groupPassword, String newState) throws IOException {
        String url = address + "management/cluster/changeState";
        return doPost(url, groupName, groupPassword, newState);
    }

    public String getClusterVersion() throws IOException {
        String url = address + "management/cluster/version";
        return doGet(url).response;
    }

    public ConnectionResponse changeClusterVersion(String groupName, String groupPassword, String version) throws IOException {
        String url = address + "management/cluster/version";
        return doPost(url, groupName, groupPassword, version);
    }

    public ConnectionResponse hotBackup(String groupName, String groupPassword) throws IOException {
        String url = address + "management/cluster/hotBackup";
        return doPost(url, groupName, groupPassword);
    }

    public ConnectionResponse hotBackupInterrupt(String groupName, String groupPassword) throws IOException {
        String url = address + "management/cluster/hotBackupInterrupt";
        return doPost(url, groupName, groupPassword);
    }

    public ConnectionResponse forceStart(String groupName, String groupPassword) throws IOException {
        String url = address + "management/cluster/forceStart";
        return doPost(url, groupName, groupPassword);
    }

    public ConnectionResponse changeManagementCenterUrl(String groupName,
                                                        String groupPassword, String newUrl) throws IOException {
        String url = address + "mancenter/changeurl";
        return doPost(url, groupName, groupPassword, newUrl);
    }

    public ConnectionResponse partialStart(String groupName, String groupPassword) throws IOException {
        String url = address + "management/cluster/partialStart";
        return doPost(url, groupName, groupPassword);
    }

    public String listClusterNodes(String groupName, String groupPassword) throws IOException {
        String url = address + "management/cluster/nodes";
        return doPost(url, groupName, groupPassword).response;
    }

    public String syncMapOverWAN(String wanRepName, String publisherId, String mapName) throws IOException {
        String url = address + "mancenter/wan/sync/map";
        return doPost(url, wanRepName, publisherId, mapName).response;
    }

    public String syncMapsOverWAN(String wanRepName, String publisherId) throws IOException {
        String url = address + "mancenter/wan/sync/allmaps";
        return doPost(url, wanRepName, publisherId).response;
    }

    public String wanMapConsistencyCheck(String wanRepName, String publisherId, String mapName) throws IOException {
        String url = address + "mancenter/wan/consistencyCheck/map";
        return doPost(url, wanRepName, publisherId, mapName).response;
    }

    public String wanPausePublisher(String wanRepName, String publisherId) throws IOException {
        String url = address + "mancenter/wan/pausePublisher";
        return doPost(url, wanRepName, publisherId).response;
    }

    public String wanStopPublisher(String wanRepName, String publisherId) throws IOException {
        String url = address + "mancenter/wan/stopPublisher";
        return doPost(url, wanRepName, publisherId).response;
    }

    public String wanResumePublisher(String wanRepName, String publisherId) throws IOException {
        String url = address + "mancenter/wan/resumePublisher";
        return doPost(url, wanRepName, publisherId).response;
    }

    public String wanClearQueues(String wanRepName, String targetGroupName) throws IOException {
        String url = address + "mancenter/wan/clearWanQueues";
        return doPost(url, wanRepName, targetGroupName).response;
    }

    public String addWanConfig(String wanRepConfigJson) throws IOException {
        String url = address + "mancenter/wan/addWanConfig";
        return doPost(url, wanRepConfigJson).response;
    }

    public String updatePermissions(String groupName, String groupPassword, String permConfJson) throws IOException {
        String url = address + "mancenter/security/permissions";
        return doPost(url, groupName, groupPassword, permConfJson).response;
    }

    static class ConnectionResponse {
        final String response;
        final int responseCode;
        final Map<String, List<String>> responseHeaders;

        private ConnectionResponse(String response, int responseCode) {
            this(response, responseCode, null);
        }

        private ConnectionResponse(String response, int responseCode, Map<String, List<String>> responseHeaders) {
            this.response = response;
            this.responseCode = responseCode;
            if (responseHeaders == null) {
                this.responseHeaders = Collections.emptyMap();
            } else {
                this.responseHeaders = new HashMap<String, List<String>>(responseHeaders);
            }
        }
    }

    private ConnectionResponse doHead(String url) throws IOException {
        CloseableHttpClient client = newClient();
        CloseableHttpResponse response = null;
        try {
            HttpHead request = new HttpHead(url);
            response = client.execute(request);

            int responseCode = response.getStatusLine().getStatusCode();

            Header[] headers = response.getAllHeaders();
            Map<String, List<String>> responseHeaders = new HashMap<String, List<String>>();
            for (Header header : headers) {
                List<String> values = responseHeaders.get(header.getName());
                if (values == null) {
                    values = new ArrayList<String>();
                    responseHeaders.put(header.getName(), values);
                }
                values.add(header.getValue());
            }

            return new ConnectionResponse(null, responseCode, responseHeaders);
        } finally {
            IOUtil.closeResource(response);
            IOUtil.closeResource(client);
        }
    }

    private ConnectionResponse doGet(String url) throws IOException {
        CloseableHttpClient client = newClient();
        CloseableHttpResponse response = null;
        try {
            HttpGet request = new HttpGet(url);
            request.setHeader("Content-type", "text/xml; charset=" + "UTF-8");
            response = client.execute(request);
            int responseCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            String responseStr = entity != null ? EntityUtils.toString(entity, "UTF-8") : "";
            return new ConnectionResponse(responseStr, responseCode);
        } finally {
            IOUtil.closeResource(response);
            IOUtil.closeResource(client);
        }
    }

    private ConnectionResponse doPost(String url, String... params) throws IOException {
        CloseableHttpClient client = newClient();

        List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>(params.length);
        for (String param : params) {
            nameValuePairs.add(new BasicNameValuePair(param, null));
        }
        String data = URLEncodedUtils.format(nameValuePairs, Consts.UTF_8);

        HttpEntity entity;
        ContentType contentType = ContentType.create("text/xml", Consts.UTF_8);
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

            int responseCode = response.getStatusLine().getStatusCode();
            String responseStr = response.getEntity() != null ? EntityUtils.toString(response.getEntity(), "UTF-8") : "";
            return new ConnectionResponse(responseStr, responseCode);
        } finally {
            IOUtil.closeResource(response);
            IOUtil.closeResource(client);
        }
    }

    private ConnectionResponse doDelete(String url) throws IOException {
        CloseableHttpClient client = newClient();
        CloseableHttpResponse response = null;
        try {
            HttpDelete request = new HttpDelete(url);
            request.setHeader("Content-type", "text/xml; charset=" + "UTF-8");
            response = client.execute(request);
            int responseCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            String responseStr = entity != null ? EntityUtils.toString(entity, "UTF-8") : "";
            return new ConnectionResponse(responseStr, responseCode);
        } finally {
            IOUtil.closeResource(response);
            IOUtil.closeResource(client);
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
        String url = address + "maps/";
        return doHead(url);
    }

    public ConnectionResponse headRequestToQueueURI() throws IOException {
        String url = address + "queues/";
        return doHead(url);
    }

    public ConnectionResponse headRequestToUndefinedURI() throws IOException {
        String url = address + "undefined";
        return doHead(url);
    }

    public ConnectionResponse getRequestToUndefinedURI() throws IOException {
        String url = address + "undefined";
        return doGet(url);
    }

    public ConnectionResponse postRequestToUndefinedURI() throws IOException {
        String url = address + "undefined";
        return doPost(url);
    }
    public ConnectionResponse deleteRequestToUndefinedURI() throws IOException {
        String url = address + "undefined";
        return doDelete(url);
    }

    public ConnectionResponse headRequestToClusterInfoURI() throws IOException {
        String url = address + "cluster";
        return doHead(url);
    }

    public ConnectionResponse getBadRequestURI() throws IOException {
        String url = address + "maps/name";
        return doGet(url);
    }

    public ConnectionResponse postBadRequestURI() throws IOException {
        String url = address + "maps/name";
        return doPost(url);
    }

    public ConnectionResponse deleteBadRequestURI() throws IOException {
        String url = address + "queues/name";
        return doDelete(url);
    }

    public ConnectionResponse headRequestToClusterHealthURI() throws IOException {
        String baseAddress = instance.getCluster().getLocalMember().getSocketAddress().toString();
        String url = "http:/" + baseAddress + HttpCommandProcessor.URI_HEALTH_URL;
        return doHead(url);
    }

    public ConnectionResponse headRequestToClusterVersionURI() throws IOException {
        String baseAddress = instance.getCluster().getLocalMember().getSocketAddress().toString();
        String url = "http:/" + baseAddress + HttpCommandProcessor.URI_CLUSTER_VERSION_URL;
        return doHead(url);
    }

    public ConnectionResponse headRequestToGarbageClusterHealthURI() throws IOException {
        String baseAddress = instance.getCluster().getLocalMember().getSocketAddress().toString();
        String url = "http:/" + baseAddress + HttpCommandProcessor.URI_HEALTH_URL + "garbage";
        return doHead(url);
    }

    public void enableChunkedStreaming() {
        this.enableChunkedStreaming = true;
    }
}
