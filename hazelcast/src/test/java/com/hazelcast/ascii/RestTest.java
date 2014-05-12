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
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * User: sancar
 * Date: 3/11/13
 * Time: 3:33 PM
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class RestTest {

    final static Config config = new XmlConfigBuilder().build();

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTtl_issue1783() throws IOException, InterruptedException {
        final Config conf = new Config();
        String name = "map";

        final CountDownLatch latch = new CountDownLatch(1);
        final MapConfig mapConfig = conf.getMapConfig(name);
        mapConfig.setTimeToLiveSeconds(3);
        mapConfig.addEntryListenerConfig(new EntryListenerConfig()
                .setImplementation(new EntryAdapter() {
                    @Override
                    public void entryEvicted(EntryEvent event) {
                        latch.countDown();
                    }
                }));

        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(conf);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);

        communicator.put(name, "key", "value");
        String value = communicator.get(name, "key");

        assertNotNull(value);
        assertEquals("value", value);

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        value = communicator.get(name, "key");
        assertTrue(value.isEmpty());
    }

    @Test
    public void testRestSimple() throws IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        final String name = "testRestSimple";
        for (int i = 0; i < 100; i++) {
            assertEquals(HttpURLConnection.HTTP_OK, communicator.put(name, String.valueOf(i), String.valueOf(i * 10)));
        }

        for (int i = 0; i < 100; i++) {
            String actual = communicator.get(name, String.valueOf(i));
            assertEquals(String.valueOf(i * 10), actual);
        }

        communicator.deleteAll(name);

        for (int i = 0; i < 100; i++) {
            String actual = communicator.get(name, String.valueOf(i));
            assertEquals("", actual);
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(HttpURLConnection.HTTP_OK, communicator.put(name, String.valueOf(i), String.valueOf(i * 10)));
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(String.valueOf(i * 10), communicator.get(name, String.valueOf(i)));
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(HttpURLConnection.HTTP_OK, communicator.delete(name, String.valueOf(i)));
        }

        for (int i = 0; i < 100; i++) {
            assertEquals("", communicator.get(name, String.valueOf(i)));
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(HttpURLConnection.HTTP_OK, communicator.offer(name, String.valueOf(i)));
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(String.valueOf(i), communicator.poll(name, 2));
        }
    }

    @Test
    public void testQueueSizeEmpty() throws IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        final String name = "testQueueSizeEmpty";

        IQueue queue = instance.getQueue(name);
        Assert.assertEquals(queue.size(), communicator.size(name));
    }

    @Test
    public void testQueueSizeNonEmpty() throws IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        final String name = "testQueueSizeNotEmpty";
        final int num_items = 100;

        IQueue queue = instance.getQueue(name);

        for (int i = 0; i < num_items; i++) {
            queue.add(i);
        }

        Assert.assertEquals(queue.size(), communicator.size(name));
    }

    private class HTTPCommunicator {

        final HazelcastInstance instance;
        final String address;

        HTTPCommunicator(HazelcastInstance instance) {
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
                rd.close();
                result = data.toString();
                httpUrlConnection.disconnect();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return result;
        }
    }
}
