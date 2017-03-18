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

package com.hazelcast.internal.ascii;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

@SuppressWarnings("SameParameterValue")
public class HTTPCommunicator {

    private final HazelcastInstance instance;
    private final String address;

    public HTTPCommunicator(HazelcastInstance instance) {
        this.instance = instance;
        this.address = "http:/" + instance.getCluster().getLocalMember().getSocketAddress().toString() + "/hazelcast/rest/";
    }

    public String queuePoll(String queueName, long timeout) throws IOException {
        String url = address + "queues/" + queueName + "/" + String.valueOf(timeout);
        return doGet(url);
    }

    public int queueSize(String queueName) throws IOException {
        String url = address + "queues/" + queueName + "/size";
        return Integer.parseInt(doGet(url));
    }

    public int queueOffer(String queueName, String data) throws IOException {
        final String url = address + "queues/" + queueName;
        final HttpURLConnection urlConnection = setupConnection(url, "POST");

        // post the data
        OutputStream out = urlConnection.getOutputStream();
        Writer writer = new OutputStreamWriter(out, "UTF-8");
        writer.write(data);
        writer.close();
        out.close();

        return urlConnection.getResponseCode();
    }

    public String mapGet(String mapName, String key) throws IOException {
        String url = address + "maps/" + mapName + "/" + key;
        return doGet(url);
    }

    public String getClusterInfo() throws IOException {
        String url = address + "cluster";
        return doGet(url);
    }

    public String getFailingClusterHealthWithTrailingGarbage() throws IOException {
        String baseAddress = instance.getCluster().getLocalMember().getSocketAddress().toString();
        String url = "http:/" + baseAddress + HttpCommandProcessor.URI_HEALTH_URL + "garbage";
        return doGet(url);
    }

    public String getClusterHealth() throws IOException {
        String baseAddress = instance.getCluster().getLocalMember().getSocketAddress().toString();
        String url = "http:/" + baseAddress + HttpCommandProcessor.URI_HEALTH_URL;
        return doGet(url);
    }

    public int mapPut(String mapName, String key, String value) throws IOException {
        final String url = address + "maps/" + mapName + "/" + key;
        final HttpURLConnection urlConnection = setupConnection(url, "POST");

        // post the data
        OutputStream out = urlConnection.getOutputStream();
        Writer writer = new OutputStreamWriter(out, "UTF-8");
        writer.write(value);
        writer.close();
        out.close();

        return urlConnection.getResponseCode();
    }

    public int mapDeleteAll(String mapName) throws IOException {
        String url = address + "maps/" + mapName;
        return setupConnection(url, "DELETE").getResponseCode();
    }

    public int mapDelete(String mapName, String key) throws IOException {
        String url = address + "maps/" + mapName + "/" + key;
        return setupConnection(url, "DELETE").getResponseCode();
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
        return doGet(url);
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

    public String syncMapOverWAN(String wanRepName, String targetGroupName, String mapName) throws IOException {
        String url = address + "mancenter/wan/sync/map";
        return doPost(url, wanRepName, targetGroupName, mapName).response;
    }

    public String syncMapsOverWAN(String wanRepName, String targetGroupName) throws IOException {
        String url = address + "mancenter/wan/sync/allmaps";
        return doPost(url, wanRepName, targetGroupName).response;
    }

    public String wanClearQueues(String wanRepName, String targetGroupName) throws IOException {
        String url = address + "mancenter/wan/clearWanQueues";
        return doPost(url, wanRepName, targetGroupName).response;
    }

    public String addWanConfig(String wanRepConfigJson) throws IOException {
        String url = address + "mancenter/wan/addWanConfig";
        return doPost(url, wanRepConfigJson).response;
    }

    private static HttpURLConnection setupConnection(String url, String method) throws IOException {
        HttpURLConnection urlConnection = (HttpURLConnection) (new URL(url)).openConnection();
        urlConnection.setRequestMethod(method);
        urlConnection.setDoOutput(true);
        urlConnection.setDoInput(true);
        urlConnection.setUseCaches(false);
        urlConnection.setAllowUserInteraction(false);
        urlConnection.setRequestProperty("Content-type", "text/xml; charset=" + "UTF-8");
        return urlConnection;
    }

    static class ConnectionResponse {
        public final String response;
        public final int responseCode;

        private ConnectionResponse(String response, int responseCode) {
            this.response = response;
            this.responseCode = responseCode;
        }
    }

    private String doGet(String url) throws IOException {
        HttpURLConnection httpUrlConnection = (HttpURLConnection) (new URL(url)).openConnection();
        try {
            InputStream inputStream = httpUrlConnection.getInputStream();
            StringBuilder builder = new StringBuilder();
            byte[] buffer = new byte[1024];
            int readBytes;
            while ((readBytes = inputStream.read(buffer)) > -1) {
                builder.append(new String(buffer, 0, readBytes));
            }
            return builder.toString();
        } finally {
            httpUrlConnection.disconnect();
        }
    }

    private static ConnectionResponse doPost(String url, String... params) throws IOException {
        HttpURLConnection urlConnection = setupConnection(url, "POST");
        // post the data
        OutputStream out = urlConnection.getOutputStream();
        Writer writer = new OutputStreamWriter(out, "UTF-8");
        String data = "";
        for (String param : params) {
            data += URLEncoder.encode(param, "UTF-8") + "&";
        }
        writer.write(data);
        writer.close();
        out.close();
        try {
            InputStream inputStream = urlConnection.getInputStream();
            byte[] buffer = new byte[4096];
            int readBytes = inputStream.read(buffer);
            return new ConnectionResponse(readBytes == -1 ? "" : new String(buffer, 0, readBytes),
                    urlConnection.getResponseCode());
        } finally {
            urlConnection.disconnect();
        }
    }
}
