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

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.ascii.HTTPCommunicator.ConnectionResponse;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.dto.WanReplicationConfigDTO;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import static com.hazelcast.internal.ascii.rest.HttpCommand.CONTENT_TYPE_JSON;
import static com.hazelcast.internal.nio.IOUtil.readFully;
import static com.hazelcast.internal.util.StringUtil.bytesToString;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastSeconds;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests HTTP REST API.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RestTest {

    private static final String MAP_WITH_TTL = "mapWithTtl";

    protected final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    protected HazelcastInstance instance;
    protected HazelcastInstance remoteInstance;
    protected HTTPCommunicator communicator;

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void setup() {
        instance = factory.newHazelcastInstance(getConfig());
        communicator = new HTTPCommunicator(instance);
    }

    public Config getConfig() {
        Config config = new Config();
        RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true).enableAllGroups();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        config.getMapConfig(MAP_WITH_TTL).setTimeToLiveSeconds(2);
        return config;
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testMapPutGet() throws Exception {
        testMapPutGet0();
    }

    @Test
    public void testMapPutGet_chunked() throws Exception {
        communicator.enableChunkedStreaming();
        testMapPutGet0();
    }

    private void testMapPutGet0() throws Exception {
        String name = randomMapName();

        String key = "key";
        String value = "value";

        assertEquals(HTTP_OK, communicator.mapPut(name, key, value));
        assertEquals(value, communicator.mapGetAndResponse(name, key));
        assertTrue(instance.getMap(name).containsKey(key));
    }

    @Test
    public void testMapPutGetByUrlEndingWithSlash() throws Exception {
        String name = randomMapName();

        String key = "key";
        String value = "value";

        assertEquals(HTTP_OK, communicator.mapPut(name, key + "/", value));
        assertEquals(value, communicator.mapGetAndResponse(name, key));
        assertEquals(value, communicator.mapGetAndResponse(name, key + "/"));
        assertTrue(instance.getMap(name).containsKey(key));
    }

    @Test
    public void testMapGetWithJson() throws IOException {
        final String mapName = "mapName";
        final String key = "key";
        String jsonValue = Json.object().add("arbitrary-attribute", "arbitrary-value").toString();
        instance.getMap(mapName).put(key, new HazelcastJsonValue(jsonValue));
        HTTPCommunicator.ConnectionResponse response = communicator.mapGet(mapName, key);

        assertContains(response.responseHeaders.get("Content-Type").iterator().next(), bytesToString(CONTENT_TYPE_JSON));
        assertEquals(jsonValue, response.response);
    }

    @Test
    public void testMapPutDelete() throws Exception {
        String name = randomMapName();

        String key = "key";
        String value = "value";

        assertEquals(HTTP_OK, communicator.mapPut(name, key, value));
        assertEquals(HTTP_OK, communicator.mapDelete(name, key));
        assertFalse(instance.getMap(name).containsKey(key));
    }

    @Test
    public void testMapPutDeleteByUrlEndingWithSlash() throws Exception {
        String name = randomMapName();

        String key = "key";
        String value = "value";

        assertEquals(HTTP_OK, communicator.mapPut(name, key, value));
        assertEquals(HTTP_OK, communicator.mapDelete(name, key + "/"));
        assertFalse(instance.getMap(name).containsKey(key));
    }

    @Test
    public void testMapDeleteAll() throws Exception {
        String name = randomMapName();

        int count = 10;
        for (int i = 0; i < count; i++) {
            assertEquals(HTTP_OK, communicator.mapPut(name, "key" + i, "value"));
        }

        IMap<Object, Object> map = instance.getMap(name);
        assertEquals(10, map.size());

        assertEquals(HTTP_OK, communicator.mapDeleteAll(name));
        assertTrue(map.isEmpty());
    }

    // issue #1783
    @Test
    public void testMapTtl() throws Exception {
        String key = "key";
        communicator.mapPut(MAP_WITH_TTL, key, "value");

        sleepAtLeastSeconds(3);

        String value = communicator.mapGetAndResponse(MAP_WITH_TTL, key);
        assertTrue(value.isEmpty());
    }

    @Test
    public void testQueueOfferPoll() throws Exception {
        String name = randomName();

        String item = communicator.queuePollAndResponse(name, 1);
        assertTrue(item.isEmpty());

        String value = "value";
        assertEquals(HTTP_OK, communicator.queueOffer(name, value));

        IQueue<Object> queue = instance.getQueue(name);
        assertEquals(1, queue.size());

        assertEquals(value, communicator.queuePollAndResponse(name, 10));
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testQueueSize() throws Exception {
        String name = randomName();
        IQueue<Integer> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.add(i);
        }

        assertEquals(queue.size(), communicator.queueSize(name));
    }

    @Test
    public void testQueuePollWithJson() throws Exception {
        final String queueName = "mapName";
        String jsonValue = Json.object().add("arbitrary-attribute", "arbitrary-value").toString();
        instance.getQueue(queueName).offer(new HazelcastJsonValue(jsonValue));
        HTTPCommunicator.ConnectionResponse response = communicator.queuePoll(queueName, 10);

        assertContains(response.responseHeaders.get("Content-Type").iterator().next(), bytesToString(CONTENT_TYPE_JSON));
        assertEquals(jsonValue, response.response);
    }

    @Test
    public void syncMapOverWAN() throws Exception {
        Config config = instance.getConfig();
        String result = communicator.syncMapOverWAN(config.getClusterName(), "", "atob", "b", "default");
        assertJsonContains(result,
                "status", "fail",
                "message", "WAN sync for map is not supported.");
    }

    @Test
    public void syncAllMapsOverWAN() throws Exception {
        Config config = instance.getConfig();
        String result = communicator.syncMapsOverWAN(config.getClusterName(), "", "atob", "b");
        assertJsonContains(result,
                "status", "fail",
                "message", "WAN sync is not supported.");
    }

    @Test
    public void wanClearQueues() throws Exception {
        Config config = instance.getConfig();
        String result = communicator.wanClearQueues(config.getClusterName(), "", "atob", "b");
        assertJsonContains(result,
                "status", "fail",
                "message", "Clearing WAN replication queues is not supported.");
    }

    @Test
    public void addWanConfig() throws Exception {
        Config config = instance.getConfig();
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName("test");
        WanReplicationConfigDTO dto = new WanReplicationConfigDTO(wanReplicationConfig);
        String result = communicator.addWanConfig(config.getClusterName(), "", dto.toJson().toString());
        assertJsonContains(result,
                "status", "fail",
                "message", "Adding new WAN config is not supported.");
    }

    @Test
    public void testMap_PutGet_withLargeValue() throws IOException {
        testMap_PutGet_withLargeValue0();
    }

    @Test
    public void testMap_PutGet_withLargeValue_chunked() throws IOException {
        communicator.enableChunkedStreaming();
        testMap_PutGet_withLargeValue0();
    }

    private void testMap_PutGet_withLargeValue0() throws IOException {
        String mapName = randomMapName();
        String key = "key";
        int capacity = 10000;
        StringBuilder value = new StringBuilder(capacity);
        while (value.length() < capacity) {
            value.append(randomString());
        }

        String valueStr = value.toString();
        int response = communicator.mapPut(mapName, key, valueStr);
        assertEquals(HTTP_OK, response);

        String actual = communicator.mapGetAndResponse(mapName, key);
        assertEquals(valueStr, actual);
    }

    @Test
    public void testMap_PutGet_withLargeKey() throws IOException {
        testMap_PutGet_withLargeKey0();
    }

    @Test
    public void testMap_PutGet_withLargeKey_chunked() throws IOException {
        communicator.enableChunkedStreaming();
        testMap_PutGet_withLargeKey0();
    }

    private void testMap_PutGet_withLargeKey0() throws IOException {
        String mapName = randomMapName();
        int capacity = 5000;
        StringBuilder key = new StringBuilder(capacity);
        while (key.length() < capacity) {
            key.append(randomString());
        }

        String value = "value";
        int response = communicator.mapPut(mapName, key.toString(), value);
        assertEquals(HTTP_OK, response);
        assertEquals(value, communicator.mapGetAndResponse(mapName, key.toString()));
    }

    @Test
    public void testMap_HeadRequest() throws IOException {
        int response = communicator.headRequestToMapURI().responseCode;
        assertEquals(HTTP_OK, response);
    }

    @Test
    public void testQueue_HeadRequest() throws IOException {
        int response = communicator.headRequestToQueueURI().responseCode;
        assertEquals(HTTP_OK, response);
    }

    @Test(expected = IOException.class)
    public void testUndefined_HeadRequest() throws IOException {
        communicator.headRequestToUndefinedURI();
    }

    @Test(expected = IOException.class)
    public void testUndefined_GetRequest() throws IOException {
        communicator.getRequestToUndefinedURI();
    }

    @Test(expected = IOException.class)
    public void testUndefined_PostRequest() throws IOException {
        communicator.postRequestToUndefinedURI();
    }

    @Test(expected = IOException.class)
    public void testUndefined_DeleteRequest() throws IOException {
        communicator.deleteRequestToUndefinedURI();
    }

    @Test
    public void testBad_GetRequest() throws IOException {
        int response = communicator.getBadRequestURI().responseCode;
        assertEquals(HTTP_BAD_REQUEST, response);
    }

    @Test
    public void testBad_PostRequest() throws IOException {
        ConnectionResponse resp = communicator.postBadRequestURI();
        assertEquals(HTTP_BAD_REQUEST, resp.responseCode);
        assertJsonContains(resp.response, "status", "fail", "message", "Missing map name");
    }

    @Test
    public void testBad_DeleteRequest() throws IOException {
        int response = communicator.deleteBadRequestURI().responseCode;
        assertEquals(HTTP_BAD_REQUEST, response);
    }

    /**
     * Regression test for <a href="https://github.com/hazelcast/hazelcast/issues/14353">Issue #14353</a>.
     */
    @Test
    public void testNoHeaders() throws IOException {
        InetSocketAddress address = getNode(instance).getLocalMember().getSocketAddress(EndpointQualifier.REST);

        getAddress(instance);
        Socket socket = new Socket(address.getAddress(), address.getPort());
        socket.setSoTimeout(5000);
        try {
            OutputStream os = socket.getOutputStream();
            os.write(stringToBytes("GET /hazelcast/rest/management/cluster/version HTTP/1.0\r\n\r\n"));
            os.flush();
            String expectedResponseHead = "HTTP/1.1 200";
            byte[] responseCode = new byte[expectedResponseHead.length()];
            readFully(socket.getInputStream(), responseCode);
            assertEquals(expectedResponseHead, bytesToString(responseCode));
        } finally {
            socket.close();
        }
    }

    private JsonObject assertJsonContains(String json, String... attributesAndValues) {
        JsonObject object = Json.parse(json).asObject();
        for (int i = 0; i < attributesAndValues.length; ) {
            String key = attributesAndValues[i++];
            String expectedValue = attributesAndValues[i++];
            assertEquals(expectedValue, object.getString(key, null));
        }
        return object;
    }

}
