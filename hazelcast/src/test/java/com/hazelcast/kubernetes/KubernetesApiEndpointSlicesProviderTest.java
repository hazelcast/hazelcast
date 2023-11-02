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

import java.util.List;
import java.util.Map;

import static com.hazelcast.kubernetes.KubernetesFakeUtils.endpointSlice;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.endpointSliceEndpoint;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.endpointSliceList;

public class KubernetesApiEndpointSlicesProviderTest
        extends KubernetesApiProviderTest {

    private static final ObjectWriter WRITER = new ObjectMapper().writer();

    public KubernetesApiEndpointSlicesProviderTest() {
        super(new KubernetesApiEndpointSlicesProvider());
    }

    @Override
    public String getEndpointsResponseWithServices() throws JsonProcessingException {
        return WRITER.writeValueAsString(endpointSliceList(
                endpointSlice("hazelcast-0", List.of("192.168.0.25"), List.of(5701), "hazelcast-0", "nodeName-1"),
                endpointSlice("service-1", List.of("172.17.0.5"), List.of(5701), "hazelcast-1", "nodeName-2"),
                endpointSlice("my-release-hazelcast", List.of(5701),
                        endpointSliceEndpoint(List.of("192.168.0.25"), "hazelcast-0", "node-name-1", true),
                        endpointSliceEndpoint(List.of("172.17.0.5"), "hazelcast-1", "node-name-2", true)),
                endpointSlice("kubernetes", List.of("34.122.156.52"), List.of(443))));
    }

    @Override
    public String getEndpointsResponse() throws JsonProcessingException {
        return WRITER.writeValueAsString(endpointSliceList(
                endpointSlice("service-0", List.of(5701),
                        endpointSliceEndpoint(List.of("172.17.0.5"), "pod-0", "nodeName-0", true),
                        endpointSliceEndpoint(List.of("192.168.0.25"), "pod-1", "node-name-1", true))));
    }

    @Override
    public String getEndpointsListResponse() throws JsonProcessingException {
        return WRITER.writeValueAsString(endpointSliceList(
                endpointSlice("service-0", Map.of("5701", 5701, "hazelcast", 5702),
                        endpointSliceEndpoint(List.of("172.17.0.5"), "pod-0", "nodeName-0", true),
                        endpointSliceEndpoint(List.of("192.168.0.25"), "pod-1", "nodeName-1", true),
                        endpointSliceEndpoint(List.of("172.17.0.6"), "pod-2", "node-name-2", false))));
    }

    @Override
    public String getEndpointsUrlString() {
        return "%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices";
    }

    @Override
    public String getEndpointsByNameUrlString() {
        return "%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices?labelSelector=kubernetes.io/service-name=%s";
    }

    @Override
    public String getEndpointsByServiceLabelUrlString() {
        return "%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices?%s";
    }
}
