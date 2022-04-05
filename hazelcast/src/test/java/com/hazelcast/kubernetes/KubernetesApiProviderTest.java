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

package com.hazelcast.kubernetes;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.kubernetes.KubernetesClient.Endpoint;
import static com.hazelcast.kubernetes.KubernetesClient.EndpointAddress;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

public abstract class KubernetesApiProviderTest {

    private KubernetesApiProvider provider;

    KubernetesApiProviderTest(KubernetesApiProvider provider) {
        this.provider = provider;
    }

    @Test
    public void getEndpointsByServiceLabelUrlStringTest() {
        //given
        assertEquals(getEndpointsByServiceLabelUrlString(), provider.getEndpointsByServiceLabelUrlString());
    }

    @Test
    public void getEndpointsByNameUrlStringTest() {
        //given
        assertEquals(getEndpointsByNameUrlString(), provider.getEndpointsByNameUrlString());
    }

    @Test
    public void getEndpointsUrlStringTest() {
        //given
        assertEquals(getEndpointsUrlString(), provider.getEndpointsUrlString());
    }

    @Test
    public void parseEndpointsList() {
        JsonObject endpointsListJson = Json.parse(getEndpointsListResponse()).asObject();
        List<Endpoint> endpoints = provider.parseEndpointsList(endpointsListJson);
        assertThat(format(endpoints),
                containsInAnyOrder(ready("192.168.0.25", 5702), ready("172.17.0.5", 5702), notReady("172.17.0.6", 5702)));

    }

    @Test
    public void parseEndpoints() {
        JsonObject endpointsListJson = Json.parse(getEndpointsResponse()).asObject();
        List<Endpoint> endpoints = provider.parseEndpoints(endpointsListJson);
        assertThat(format(endpoints), containsInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5701)));

    }

    @Test
    public void extractServices() {
        //given
        JsonObject endpointsJson = Json.parse(getEndpointsResponseWithServices()).asObject();
        ArrayList<EndpointAddress> privateAddresses = new ArrayList<>();
        privateAddresses.add(new EndpointAddress("192.168.0.25", 5701));
        privateAddresses.add(new EndpointAddress("172.17.0.5", 5701));
        //when
        Map<EndpointAddress, String> services = provider.extractServices(endpointsJson, privateAddresses);
        //then
        assertThat(format(services), containsInAnyOrder(toString("192.168.0.25", 5701, "hazelcast-0"),
                toString("172.17.0.5", 5701, "service-1")));
    }

    @Test
    public void extractNodes() {
        //given
        JsonObject endpointsJson = Json.parse(getEndpointsResponseWithServices()).asObject();
        ArrayList<EndpointAddress> privateAddresses = new ArrayList<>();
        privateAddresses.add(new EndpointAddress("192.168.0.25", 5701));
        privateAddresses.add(new EndpointAddress("172.17.0.5", 5701));
        //when
        Map<EndpointAddress, String> nodes = provider.extractNodes(endpointsJson, privateAddresses);
        //then
        assertThat(format(nodes), containsInAnyOrder(toString("192.168.0.25", 5701, "node-name-1"),
                toString("172.17.0.5", 5701, "node-name-2")));
    }

    private static List<String> format(List<Endpoint> addresses) {
        List<String> result = new ArrayList<>();
        for (Endpoint address : addresses) {
            String ip = address.getPrivateAddress().getIp();
            Integer port = address.getPrivateAddress().getPort();
            boolean isReady = address.isReady();
            result.add(toString(ip, port, isReady));
        }
        return result;
    }

    private static List<String> format(Map<EndpointAddress, String> addresses) {
        List<String> result = new ArrayList<>();
        for (EndpointAddress address : addresses.keySet()) {
            String ip = address.getIp();
            Integer port = address.getPort();
            String service = addresses.get(address);
            result.add(toString(ip, port, service));
        }
        return result;
    }

    private static String toString(String ip, Integer port, String endpointName) {
        return String.format("%s:%s:%s", ip, port, endpointName);
    }

    private static String toString(String ip, Integer port, boolean isReady) {
        return String.format("%s:%s:%s", ip, port, isReady);
    }

    private static String ready(String ip, Integer port) {
        return toString(ip, port, true);
    }

    private static String notReady(String ip, Integer port) {
        return toString(ip, port, false);
    }

    public abstract String getEndpointsResponseWithServices();

    public abstract String getEndpointsResponse();

    public abstract String getEndpointsListResponse();

    public abstract String getEndpointsUrlString();

    public abstract String getEndpointsByNameUrlString();

    public abstract String getEndpointsByServiceLabelUrlString();
}
