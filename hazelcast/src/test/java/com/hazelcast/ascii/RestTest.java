/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ascii;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * User: sancar
 * Date: 3/11/13
 * Time: 3:33 PM
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)

public class RestTest {

    final static Config config = new XmlConfigBuilder().build();

    @After
    @Before
    public void shutdownAll() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testRestSimple() throws IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        final String name = "testRestSimple";
        for (int i = 0; i < 100; i++) {
            communicator.put(name, String.valueOf(i), String.valueOf(i * 10));
        }

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i * 10), communicator.get(name, String.valueOf(i)));
        }

        for (int i = 0; i < 100; i++) {
            communicator.delete(name, String.valueOf(i));
        }

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals("", communicator.get(name, String.valueOf(i)));
        }

        for (int i = 0; i < 100; i++) {
            communicator.offer(name, String.valueOf(i));
        }

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), communicator.poll(name, 2));
        }
    }

    private class HTTPCommunicator {

        final HazelcastInstance instance;
        final String address;

        HTTPCommunicator(HazelcastInstance instance) {
            this.instance = instance;
            address = "http:/" + instance.getCluster().getLocalMember().getInetSocketAddress().toString() + "/hazelcast/rest/";
        }

        public String poll(String queueName, long timeout) {
            String url = address + "queues/" + queueName + "/" + String.valueOf(timeout);
            String result = null;
            try {
                HttpURLConnection httpUrlConnection = (HttpURLConnection) (new URL(url)).openConnection();
                // Get the response
                BufferedReader rd = new BufferedReader(new InputStreamReader(httpUrlConnection.getInputStream()));
                StringBuilder data = new StringBuilder(150);
                String line;
                while ((line = rd.readLine()) != null) data.append(line);
                rd.close();
                result = data.toString();
                httpUrlConnection.disconnect();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return result;

        }

        public boolean offer(String queueName, String data) throws IOException {
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

            /** read the response back from the posted data */
            BufferedReader reader = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
            StringBuilder builder = new StringBuilder(100);
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
            reader.close();

            return Boolean.valueOf(builder.toString());
        }

        public String get(String mapName, String key) {
            String url = address + "maps/" + mapName + "/" + key;
            String result = null;
            try {
                HttpURLConnection httpUrlConnection = (HttpURLConnection) (new URL(url)).openConnection();
                // Get the response
                BufferedReader rd = new BufferedReader(new InputStreamReader(httpUrlConnection.getInputStream()));
                StringBuilder data = new StringBuilder(150);
                String line;
                while ((line = rd.readLine()) != null) data.append(line);
                rd.close();
                result = data.toString();
                httpUrlConnection.disconnect();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return result;
        }

        public String put(String mapName, String key, String value) throws IOException {

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
            OutputStream out = null;
            out = urlConnection.getOutputStream();
            Writer writer = new OutputStreamWriter(out, "UTF-8");
            writer.write(value);
            writer.close();
            out.close();

            /** read the response back from the posted data */
            BufferedReader reader = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
            StringBuilder builder = new StringBuilder(100);
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
            reader.close();

            return builder.toString();
        }

        public String delete(String mapName, String key) throws IOException {

            String url = address + "maps/" + mapName + "/" + key;
            /** set up the http connection parameters */
            HttpURLConnection urlConnection = (HttpURLConnection) (new URL(url)).openConnection();
            urlConnection.setRequestMethod("DELETE");
            urlConnection.setDoOutput(true);
            urlConnection.setDoInput(true);
            urlConnection.setUseCaches(false);
            urlConnection.setAllowUserInteraction(false);
            urlConnection.setRequestProperty("Content-type", "text/xml; charset=" + "UTF-8");

            /** read the response back from the posted data */
            BufferedReader reader = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
            StringBuilder builder = new StringBuilder(100);
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
            reader.close();

            return builder.toString();
        }

    }
}
