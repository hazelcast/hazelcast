/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dataconnection.HazelcastDataConnection;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.pipeline.DataConnectionRef.dataConnectionRef;
import static java.nio.file.Files.readAllBytes;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class})
public class SinksRemoteMapWithUpdatingTest extends PipelineTestSupport {

    private static HazelcastInstance remoteHz;
    private static ClientConfig clientConfig;

    private static final String HZ_CLIENT_DATA_CONNECTION_NAME = "hzclientexternalref";

    @BeforeClass
    public static void setUp() throws IOException {
        // Create remote cluster
        String clusterName = randomName();

        Config remoteClusterConfig = new Config();
        remoteClusterConfig.setClusterName(clusterName);
        remoteClusterConfig.addCacheConfig(new CacheSimpleConfig().setName("*"));
        remoteHz = createRemoteCluster(remoteClusterConfig, 2).get(0);

        clientConfig = getClientConfigForRemoteCluster(remoteHz);

        // Create local cluster
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig(HZ_CLIENT_DATA_CONNECTION_NAME);
        dataConnectionConfig.setType("HZ");

        // Read XML and set as DataConnectionConfig
        String xmlString = readLocalClusterConfig("hazelcast-client-test-external.xml", clusterName);
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML, xmlString);

        for (HazelcastInstance hazelcastInstance : allHazelcastInstances()) {
            Config hazelcastInstanceConfig = hazelcastInstance.getConfig();
            hazelcastInstanceConfig.addDataConnectionConfig(dataConnectionConfig);
        }
    }

    private static String readLocalClusterConfig(String file, String clusterName) throws IOException {
        byte[] bytes = readAllBytes(Paths.get("src", "test", "resources", file));
        return new String(bytes, StandardCharsets.UTF_8)
                .replace("$CLUSTER_NAME$", clusterName);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void remoteMapWithUpdating() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMapWithUpdating(
                srcName,
                clientConfig,
                Entry::getKey,
                (Integer value, Entry<String, Integer> item) -> value + 10);

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                .map(i -> entry(String.valueOf(i), i + 10))
                .collect(toList());
        Set<Entry<String, Integer>> actual = remoteHz.<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMapWithUpdating_withExternalConfig() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMapWithUpdating(
                srcName,
                dataConnectionRef(HZ_CLIENT_DATA_CONNECTION_NAME),
                Entry::getKey,
                (Integer value, Entry<String, Integer> item) -> value + 10);

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                .map(i -> entry(String.valueOf(i), i + 10))
                .collect(toList());
        Set<Entry<String, Integer>> actual = remoteHz.<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMapWithUpdating_when_functionReturnsNull_then_keyIsRemoved() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMapWithUpdating(srcName,
                clientConfig,
                (value, item) -> null);

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        Set<Entry<Object, Object>> actual = remoteHz.getMap(srcName).entrySet();
        assertEquals(0, actual.size());
    }

    @Test
    public void remoteMapWithUpdating_when_functionReturnsNull_then_keyIsRemoved_withExternalConfig() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMapWithUpdating(srcName,
                dataConnectionRef(HZ_CLIENT_DATA_CONNECTION_NAME),
                (value, item) -> null);

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        Set<Entry<Object, Object>> actual = remoteHz.getMap(srcName).entrySet();
        assertEquals(0, actual.size());
    }

    @Test
    public void remoteMapWithUpdating_when_itemDataSerializable() {
        // Given
        IMap<String, DataSerializableObject> sourceMap = remoteHz.getMap(srcName);
        List<Integer> input = sequence(itemCount);
        input.forEach(i -> sourceMap.put(String.valueOf(i), new DataSerializableObject(i)));

        // When
        Sink<Entry<String, DataSerializableObject>> sink = Sinks.remoteMapWithUpdating(
                srcName,
                clientConfig,
                (value, item) -> new DataSerializableObject(value.value + item.getValue().value));

        // Then
        p.readFrom(Sources.<String, DataSerializableObject>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, DataSerializableObject>> expected = input
                .stream()
                .map(i -> entry(String.valueOf(i), new DataSerializableObject(i * 2)))
                .collect(toList());
        Set<Entry<String, DataSerializableObject>> actual = sourceMap.entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMapWithUpdating_when_itemDataSerializable_withExternalConfig() {
        // Given
        IMap<String, DataSerializableObject> sourceMap = remoteHz.getMap(srcName);
        List<Integer> input = sequence(itemCount);
        input.forEach(i -> sourceMap.put(String.valueOf(i), new DataSerializableObject(i)));

        // When
        Sink<Entry<String, DataSerializableObject>> sink = Sinks.remoteMapWithUpdating(
                srcName,
                dataConnectionRef(HZ_CLIENT_DATA_CONNECTION_NAME),
                (value, item) -> new DataSerializableObject(value.value + item.getValue().value));

        // Then
        p.readFrom(Sources.<String, DataSerializableObject>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, DataSerializableObject>> expected = input
                .stream()
                .map(i -> entry(String.valueOf(i), new DataSerializableObject(i * 2)))
                .collect(toList());
        Set<Entry<String, DataSerializableObject>> actual = sourceMap.entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    private static class DataSerializableObject implements DataSerializable {
        int value;

        DataSerializableObject() {
        }

        DataSerializableObject(int value) {
            this.value = value;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            value = in.readInt();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DataSerializableObject that = (DataSerializableObject) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }
}
