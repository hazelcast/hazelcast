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

package com.hazelcast.kubernetes;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.util.Collections;
import java.util.HashMap;

import static com.hazelcast.kubernetes.KubernetesFakeUtils.endpointAddress;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.endpoints;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.endpointsList;

public class KubernetesApiEndpointProviderTest
        extends KubernetesApiProviderTest {

    private static final ObjectWriter WRITER = new ObjectMapper().writer();

    public KubernetesApiEndpointProviderTest() {
        super(new KubernetesApiEndpointProvider());
    }

    public String getEndpointsResponseWithServices() throws JsonProcessingException {
        return WRITER.writeValueAsString(endpointsList(
                endpoints("my-release-hazelcast", 5701,
                        endpointAddress("192.168.0.25", "hazelcast-0", "node-name-1"),
                        endpointAddress("172.17.0.5", "hazelcast-1", "node-name-2")),
                endpoints("service-0", 5701,
                        endpointAddress("192.168.0.25", "hazelcast-0", "node-name-1")),
                endpoints("hazelcast-0", 5701,
                        endpointAddress("192.168.0.25", "hazelcast-0", "node-name-1")),
                endpoints("service-1", 5701,
                        endpointAddress("172.17.0.5", "hazelcast-1", "node-name-2")),
                endpoints("kubernetes", "192.168.49.2", 443)));
    }

    public String getEndpointsResponse() throws JsonProcessingException {
        return WRITER.writeValueAsString(
                endpoints(new HashMap<String, String>() {{
                    put("192.168.0.25", "hazelcast-0");
                    put("172.17.0.5", "hazelcast-1");
                }}, 5701));
    }

    public String getEndpointsListResponse() throws JsonProcessingException {
        return WRITER.writeValueAsString(endpointsList(
                endpoints(new HashMap<String, String>() {{
                              put("172.17.0.5", "hazelcast-0");
                              put("192.168.0.25", "hazelcast-1");
                          }},
                        Collections.singletonMap("172.17.0.6", "hazelcast-2"),
                        new HashMap<String, Integer>() {{
                            put("5701", 5701);
                            put("hazelcast", 5702);
                        }})));
    }

    public String getEndpointsUrlString() {
        return "%s/api/v1/namespaces/%s/endpoints";
    }

    public String getEndpointsByNameUrlString() {
        return "%s/api/v1/namespaces/%s/endpoints/%s";
    }

    public String getEndpointsByServiceLabelUrlString() {
        return "%s/api/v1/namespaces/%s/endpoints?%s";
    }
}
