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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;

public class HTTPCommunicator {

    final HazelcastInstance instance;
    final String address;

    public HTTPCommunicator(HazelcastInstance instance) {
        this.instance = instance;
        this.address = "http:/" + instance.getCluster().getLocalMember().getInetSocketAddress().toString() + "/hazelcast/rest/";
    }

    public String poll(String queueName, long timeout) {
        String url = address + "queues/" + queueName + "/" + String.valueOf(timeout);
        String result = doGet(url);
        return result;
    }

    public int size(String queueName) {
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

    public String get(String mapName, String key) {
        String url = address + "maps/" + mapName + "/" + key;
        String result = doGet(url);
        return result;
    }

    public String getClusterInfo() {
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

    private String doGet(final String url) {
        String result = null;
        try {
            HttpURLConnection httpUrlConnection = (HttpURLConnection) (new URL(url)).openConnection();
            BufferedReader rd = new BufferedReader(new InputStreamReader(httpUrlConnection.getInputStream()));
            StringBuilder data = new StringBuilder(150);
            String line;
            while ((line = rd.readLine()) != null) data.append(line);
            result = data.toString();
            httpUrlConnection.disconnect();
            rd.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
