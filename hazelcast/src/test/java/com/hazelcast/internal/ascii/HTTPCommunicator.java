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

package com.hazelcast.internal.ascii;

import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

public class HTTPCommunicator {

    final HazelcastInstance instance;
    final String address;

    public HTTPCommunicator(HazelcastInstance instance) {
        this.instance = instance;
        this.address = "http:/" + instance.getCluster().getLocalMember().getSocketAddress().toString() + "/hazelcast/rest/";
    }

    public String poll(String queueName, long timeout) throws IOException {
        String url = address + "queues/" + queueName + "/" + String.valueOf(timeout);
        String result = doGet(url);
        return result;
    }

    public int size(String queueName) throws IOException {
        String url = address + "queues/" + queueName + "/size";
        Integer result = Integer.parseInt(doGet(url));
        return result;
    }

    public int offer(String queueName, String data) throws IOException {
        String url = address + "queues/" + queueName;
        /** set up the http connection parameters */
        HttpURLConnection urlConnection = (HttpURLConnection) (new URL(url)).openConnection();
        urlConnection.setRequestMethod("POST");
        urlConnection.setDoOutput(true);
        urlConnection.setDoInput(true);
        urlConnection.setUseCaches(false);
        urlConnection.setAllowUserInteraction(false);
        urlConnection.setRequestProperty("Content-type", "text/xml; charset=" + "UTF-8");

        /** post the data */
        OutputStream out = null;
        out = urlConnection.getOutputStream();
        Writer writer = new OutputStreamWriter(out, "UTF-8");
        writer.write(data);
        writer.close();
        out.close();


        return urlConnection.getResponseCode();
    }

    public String get(String mapName, String key) throws IOException {
        String url = address + "maps/" + mapName + "/" + key;
        String result = doGet(url);
        return result;
    }

    public String getClusterInfo() throws IOException {
        String url = address + "cluster";
        return doGet(url);

    }

    public int put(String mapName, String key, String value) throws IOException {

        String url = address + "maps/" + mapName + "/" + key;
        /** set up the http connection parameters */
        HttpURLConnection urlConnection = (HttpURLConnection) (new URL(url)).openConnection();
        urlConnection.setRequestMethod("POST");
        urlConnection.setDoOutput(true);
        urlConnection.setDoInput(true);
        urlConnection.setUseCaches(false);
        urlConnection.setAllowUserInteraction(false);
        urlConnection.setRequestProperty("Content-type", "text/xml; charset=" + "UTF-8");

        /** post the data */
        OutputStream out = urlConnection.getOutputStream();
        Writer writer = new OutputStreamWriter(out, "UTF-8");
        writer.write(value);
        writer.close();
        out.close();

        return urlConnection.getResponseCode();
    }

    public int deleteAll(String mapName) throws IOException {

        String url = address + "maps/" + mapName;
        /** set up the http connection parameters */
        HttpURLConnection urlConnection = (HttpURLConnection) (new URL(url)).openConnection();
        urlConnection.setRequestMethod("DELETE");
        urlConnection.setDoOutput(true);
        urlConnection.setDoInput(true);
        urlConnection.setUseCaches(false);
        urlConnection.setAllowUserInteraction(false);
        urlConnection.setRequestProperty("Content-type", "text/xml; charset=" + "UTF-8");

        return urlConnection.getResponseCode();
    }

    public int delete(String mapName, String key) throws IOException {

        String url = address + "maps/" + mapName + "/" + key;
        /** set up the http connection parameters */
        HttpURLConnection urlConnection = (HttpURLConnection) (new URL(url)).openConnection();
        urlConnection.setRequestMethod("DELETE");
        urlConnection.setDoOutput(true);
        urlConnection.setDoInput(true);
        urlConnection.setUseCaches(false);
        urlConnection.setAllowUserInteraction(false);
        urlConnection.setRequestProperty("Content-type", "text/xml; charset=" + "UTF-8");

        return urlConnection.getResponseCode();
    }

    public int shutdownCluster(String groupName, String groupPassword) throws IOException {

        String url = address + "management/cluster/shutdown";
        /** set up the http connection parameters */
        HttpURLConnection urlConnection = (HttpURLConnection) (new URL(url)).openConnection();
        urlConnection.setRequestMethod("POST");
        urlConnection.setDoOutput(true);
        urlConnection.setDoInput(true);
        urlConnection.setUseCaches(false);
        urlConnection.setAllowUserInteraction(false);
        urlConnection.setRequestProperty("Content-type", "text/xml; charset=" + "UTF-8");

        /** post the data */
        OutputStream out = urlConnection.getOutputStream();
        Writer writer = new OutputStreamWriter(out, "UTF-8");
        String data = URLEncoder.encode(groupName, "UTF-8") + "&" + URLEncoder.encode(groupPassword, "UTF-8");
        writer.write(data);
        writer.close();
        out.close();

        return urlConnection.getResponseCode();
    }

    public String getClusterState(String groupName, String groupPassword) throws IOException {

        String url = address + "management/cluster/state";
        return doPost(url, groupName, groupPassword);

    }

    public int changeClusterState(String groupName, String groupPassword, String newState) throws IOException {

        String url = address + "management/cluster/changeState";
        /** set up the http connection parameters */
        HttpURLConnection urlConnection = (HttpURLConnection) (new URL(url)).openConnection();
        urlConnection.setRequestMethod("POST");
        urlConnection.setDoOutput(true);
        urlConnection.setDoInput(true);
        urlConnection.setUseCaches(false);
        urlConnection.setAllowUserInteraction(false);
        urlConnection.setRequestProperty("Content-type", "text/xml; charset=" + "UTF-8");

        /** post the data */
        OutputStream out = urlConnection.getOutputStream();
        Writer writer = new OutputStreamWriter(out, "UTF-8");
        String data = URLEncoder.encode(groupName, "UTF-8") + "&" + URLEncoder.encode(groupPassword, "UTF-8")+
                "&" + URLEncoder.encode(newState, "UTF-8");
        writer.write(data);
        writer.close();
        out.close();

        return urlConnection.getResponseCode();
    }

    private String doGet(final String url) throws IOException {
        HttpURLConnection httpUrlConnection = (HttpURLConnection) (new URL(url)).openConnection();
        try {
            InputStream inputStream = httpUrlConnection.getInputStream();
            byte[] buffer = new byte[4096];
            int readBytes = inputStream.read(buffer);
            return readBytes == -1 ? "" : new String(buffer, 0, readBytes);
        } finally {
            httpUrlConnection.disconnect();
        }
    }

    private String doPost(final String url, String ... params) throws IOException {
        /** set up the http connection parameters */
        HttpURLConnection urlConnection = (HttpURLConnection) (new URL(url)).openConnection();
        urlConnection.setRequestMethod("POST");
        urlConnection.setDoOutput(true);
        urlConnection.setDoInput(true);
        urlConnection.setUseCaches(false);
        urlConnection.setAllowUserInteraction(false);
        urlConnection.setRequestProperty("Content-type", "text/xml; charset=" + "UTF-8");
        /** post the data */
        OutputStream out = urlConnection.getOutputStream();
        Writer writer = new OutputStreamWriter(out, "UTF-8");
        String data = "";
        for ( String param : params){
            data +=  URLEncoder.encode(param, "UTF-8") + "&";
        }
        writer.write(data);
        writer.close();
        out.close();
        try {
            InputStream inputStream = urlConnection.getInputStream();
            byte[] buffer = new byte[4096];
            int readBytes = inputStream.read(buffer);
            return readBytes == -1 ? "" : new String(buffer, 0, readBytes);
        } finally {
            urlConnection.disconnect();
        }
    }
}
