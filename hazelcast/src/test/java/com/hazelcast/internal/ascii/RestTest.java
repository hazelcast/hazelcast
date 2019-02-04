/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.internal.management.dto.WanReplicationConfigDTO;
import com.hazelcast.internal.management.request.UpdatePermissionConfigRequest;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.nio.IOUtil.readFully;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastSeconds;
import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.StringUtil.stringToBytes;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
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

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    private HazelcastInstance instance;
    private HTTPCommunicator communicator;

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.shutdownAll();
    }

    private Config setup() {
        Config config = new Config();
        RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true).enableAllGroups();
        config.setRestApiConfig(restApiConfig);
        instance = factory.newHazelcastInstance(config);
        communicator = new HTTPCommunicator(instance);
        return config;
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testMapPutGet() throws Exception {
        setup();
        testMapPutGet0();
    }

    @Test
    public void testMapPutGet_chunked() throws Exception {
        setup();
        communicator.enableChunkedStreaming();
        testMapPutGet0();
    }

    private void testMapPutGet0() throws Exception {
        String name = randomMapName();

        String key = "key";
        String value = "value";

        assertEquals(HTTP_OK, communicator.mapPut(name, key, value));
        assertEquals(value, communicator.mapGet(name, key));
        assertTrue(instance.getMap(name).containsKey(key));
    }

    @Test
    public void testMapPutDelete() throws Exception {
        setup();
        String name = randomMapName();

        String key = "key";
        String value = "value";

        assertEquals(HTTP_OK, communicator.mapPut(name, key, value));
        assertEquals(HTTP_OK, communicator.mapDelete(name, key));
        assertFalse(instance.getMap(name).containsKey(key));
    }

    @Test
    public void testMapDeleteAll() throws Exception {
        setup();
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
        Config config = setup();
        String name = randomMapName();
        config.getMapConfig(name)
                .setTimeToLiveSeconds(2);

        String key = "key";
        communicator.mapPut(name, key, "value");

        sleepAtLeastSeconds(3);

        String value = communicator.mapGet(name, key);
        assertTrue(value.isEmpty());
    }

    @Test
    public void testQueueOfferPoll() throws Exception {
        setup();
        String name = randomName();

        String item = communicator.queuePoll(name, 1);
        assertTrue(item.isEmpty());

        String value = "value";
        assertEquals(HTTP_OK, communicator.queueOffer(name, value));

        IQueue<Object> queue = instance.getQueue(name);
        assertEquals(1, queue.size());

        assertEquals(value, communicator.queuePoll(name, 10));
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testQueueSize() throws Exception {
        setup();
        String name = randomName();
        IQueue<Integer> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.add(i);
        }

        assertEquals(queue.size(), communicator.queueSize(name));
    }

    @Test
    public void syncMapOverWAN() throws Exception {
        setup();
        String result = communicator.syncMapOverWAN("atob", "b", "default");
        assertEquals("{\"status\":\"fail\",\"message\":\"WAN sync for map is not supported.\"}", result);
    }

    @Test
    public void syncAllMapsOverWAN() throws Exception {
        setup();
        String result = communicator.syncMapsOverWAN("atob", "b");
        assertEquals("{\"status\":\"fail\",\"message\":\"WAN sync is not supported.\"}", result);
    }

    @Test
    public void wanClearQueues() throws Exception {
        setup();
        String result = communicator.wanClearQueues("atob", "b");
        assertEquals("{\"status\":\"fail\",\"message\":\"Clearing WAN replication queues is not supported.\"}", result);
    }

    @Test
    public void addWanConfig() throws Exception {
        setup();
        WanReplicationConfig wanConfig = new WanReplicationConfig();
        wanConfig.setName("test");
        WanReplicationConfigDTO dto = new WanReplicationConfigDTO(wanConfig);
        String result = communicator.addWanConfig(dto.toJson().toString());
        assertEquals("{\"status\":\"fail\",\"message\":\"Adding new WAN config is not supported.\"}", result);
    }

    @Test
    public void updatePermissions() throws Exception {
        Config config = setup();
        Set<PermissionConfig> permissionConfigs = new HashSet<PermissionConfig>();
        permissionConfigs.add(new PermissionConfig(PermissionConfig.PermissionType.MAP, "test", "*"));
        UpdatePermissionConfigRequest request = new UpdatePermissionConfigRequest(permissionConfigs);
        String result = communicator.updatePermissions(config.getGroupConfig().getName(),
                "", request.toJson().toString());
        assertEquals("{\"status\":\"forbidden\"}", result);
    }

    @Test
    public void testMap_PutGet_withLargeValue() throws IOException {
        setup();
        testMap_PutGet_withLargeValue0();
    }

    @Test
    public void testMap_PutGet_withLargeValue_chunked() throws IOException {
        setup();
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

        String actual = communicator.mapGet(mapName, key);
        assertEquals(valueStr, actual);
    }

    @Test
    public void testMap_PutGet_withLargeKey() throws IOException {
        setup();
        testMap_PutGet_withLargeKey0();
    }

    @Test
    public void testMap_PutGet_withLargeKey_chunked() throws IOException {
        setup();
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
        assertEquals(value, communicator.mapGet(mapName, key.toString()));
    }

    @Test
    public void testMap_HeadRequest() throws IOException {
        setup();
        int response = communicator.headRequestToMapURI().responseCode;
        assertEquals(HTTP_OK, response);
    }

    @Test
    public void testQueue_HeadRequest() throws IOException {
        setup();
        int response = communicator.headRequestToQueueURI().responseCode;
        assertEquals(HTTP_OK, response);
    }

    @Test
    public void testUndefined_HeadRequest() throws IOException {
        setup();
        int response = communicator.headRequestToUndefinedURI().responseCode;
        assertEquals(HTTP_NOT_FOUND, response);
    }

    @Test
    public void testUndefined_GetRequest() throws IOException {
        setup();
        int response = communicator.getRequestToUndefinedURI().responseCode;
        assertEquals(HTTP_NOT_FOUND, response);
    }

    @Test
    public void testUndefined_PostRequest() throws IOException {
        setup();
        int response = communicator.postRequestToUndefinedURI().responseCode;
        assertEquals(HTTP_NOT_FOUND, response);
    }

    @Test
    public void testUndefined_DeleteRequest() throws IOException {
        setup();
        int response = communicator.deleteRequestToUndefinedURI().responseCode;
        assertEquals(HTTP_NOT_FOUND, response);
    }

    @Test
    public void testBad_GetRequest() throws IOException {
        setup();
        int response = communicator.getBadRequestURI().responseCode;
        assertEquals(HTTP_BAD_REQUEST, response);
    }

    @Test
    public void testBad_PostRequest() throws IOException {
        setup();
        int response = communicator.postBadRequestURI().responseCode;
        assertEquals(HTTP_BAD_REQUEST, response);
    }

    @Test
    public void testBad_DeleteRequest() throws IOException {
        setup();
        int response = communicator.deleteBadRequestURI().responseCode;
        assertEquals(HTTP_BAD_REQUEST, response);
    }

    /**
     * Regression test for <a href="https://github.com/hazelcast/hazelcast/issues/14353">Issue #14353</a>.
     */
    @Test
    public void testNoHeaders() throws IOException {
        setup();
        Address address = HazelcastTestSupport.getAddress(instance);
        Socket socket = new Socket(address.getInetAddress(), address.getPort());
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

}
