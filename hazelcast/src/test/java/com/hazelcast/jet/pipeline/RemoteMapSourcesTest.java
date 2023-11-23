/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.pipeline.DataConnectionRef.dataConnectionRef;
import static com.hazelcast.projection.Projections.singleAttribute;
import static com.hazelcast.query.impl.predicates.TruePredicate.truePredicate;
import static java.nio.file.Files.readAllBytes;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(QuickTest.class)
public class RemoteMapSourcesTest extends PipelineTestSupport {
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
    public void remoteMap() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        BatchSource<Entry<String, Integer>> source = Sources.remoteMap(srcName, clientConfig);

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                .map(i -> entry(String.valueOf(i), i))
                .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void remoteMap_withExternalConfig() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        BatchSource<Entry<Object, Object>> source = Sources.remoteMap(srcName,
                dataConnectionRef(HZ_CLIENT_DATA_CONNECTION_NAME));

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                .map(i -> entry(String.valueOf(i), i))
                .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void remoteMapWithFilterAndProjection() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        BatchSource<?> source = Sources.remoteMap(
                srcName, clientConfig,
                truePredicate(),
                singleAttribute("value"));

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void remoteMapWithFilterAndProjection_withExternalConfig() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        BatchSource<?> source = Sources.remoteMap(
                srcName,
                dataConnectionRef(HZ_CLIENT_DATA_CONNECTION_NAME),
                truePredicate(),
                singleAttribute("value"));

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void remoteMapWithFilterAndProjection_withExternalConfig_usingBuilder() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        BatchSource<?> source = Sources.remoteMapBuilder(srcName)
                .dataConnectionName(HZ_CLIENT_DATA_CONNECTION_NAME)
                .predicate(truePredicate())
                .projection(singleAttribute("value"))
                .build();

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void remoteMapWithFilterAndProjectionFn() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        BatchSource<? extends Integer> source = Sources.remoteMap(
                srcName, clientConfig, truePredicate(), Entry<String, Integer>::getValue);

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void remoteMapWithFilterAndProjectionFn_withExternalConfig() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        BatchSource<Integer> source = Sources.remoteMap(
                srcName,
                dataConnectionRef(HZ_CLIENT_DATA_CONNECTION_NAME),
                truePredicate(),
                Entry<String, Integer>::getValue);

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void remoteMapWithUnknownValueClass_whenQueryingIsNotNecessary() throws Exception {
        // Given
        URL jarResource = Thread.currentThread().getContextClassLoader()
                .getResource("deployment/sample-pojo-1.0-car.jar");
        assertNotNull("jar not found", jarResource);
        Class<?> carClz;
        try (URLClassLoader cl = new URLClassLoader(new URL[]{jarResource})) {
            carClz = cl.loadClass("com.sample.pojo.car.Car");
        }
        Object carPojo = carClz.getConstructor(String.class, String.class)
                .newInstance("make", "model");
        IMap<String, Object> map = remoteHz.getMap(srcName);
        // the class of the value is unknown to the remote IMDG member, it will be only known to Jet
        map.put("key", carPojo);

        // When
        BatchSource<Entry<String, Object>> source = Sources.remoteMap(srcName, clientConfig);

        // Then
        p.readFrom(source).map(en -> en.getValue().toString()).writeTo(sink);
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJar(jarResource);
        hz().getJet().newJob(p, jobConfig).join();
        List<Object> expected = singletonList(carPojo.toString());
        List<Object> actual = new ArrayList<>(sinkList);
        assertEquals(expected, actual);
    }

    @Test
    public void remoteMapWithUnknownValueClass_whenQueryingIsNotNecessary_withExternalConfig() throws Exception {
        // Given
        URL jarResource = Thread.currentThread().getContextClassLoader()
                .getResource("deployment/sample-pojo-1.0-car.jar");
        assertNotNull("jar not found", jarResource);
        Class<?> carClz;
        try (URLClassLoader cl = new URLClassLoader(new URL[]{jarResource})) {
            carClz = cl.loadClass("com.sample.pojo.car.Car");
        }
        Object carPojo = carClz.getConstructor(String.class, String.class)
                .newInstance("make", "model");
        IMap<String, Object> map = remoteHz.getMap(srcName);
        // the class of the value is unknown to the remote IMDG member, it will be only known to Jet
        map.put("key", carPojo);

        // When
        BatchSource<Entry<String, Object>> source = Sources.remoteMap(srcName,
                dataConnectionRef(HZ_CLIENT_DATA_CONNECTION_NAME));

        // Then
        p.readFrom(source).map(en -> en.getValue().toString()).writeTo(sink);
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJar(jarResource);
        hz().getJet().newJob(p, jobConfig).join();
        List<Object> expected = singletonList(carPojo.toString());
        List<Object> actual = new ArrayList<>(sinkList);
        assertEquals(expected, actual);
    }

}
