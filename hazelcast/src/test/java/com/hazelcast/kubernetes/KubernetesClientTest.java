/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.kubernetes.KubernetesClient.Endpoint;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KubernetesClientTest {
    private static final String KUBERNETES_MASTER_IP = "localhost";

    private static final String TOKEN = "sample-token";
    private static final String CA_CERTIFICATE = "sample-ca-certificate";
    private static final String NAMESPACE = "sample-namespace";
    private static final int RETRIES = 3;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    private KubernetesClient kubernetesClient;

    @Before
    public void setUp() {
        kubernetesClient = newKubernetesClient(false);
        stubFor(get(urlMatching("/api/.*")).atPriority(5)
                .willReturn(aResponse().withStatus(401).withBody("\"reason\":\"Forbidden\"")));
    }

    @Test
    public void endpointsByNamespace() {
        // given
        //language=JSON
        String podsListResponse = "{\n"
                + "  \"items\": [\n"
                + "    {\n"
                + "      \"spec\": {\n"
                + "        \"containers\": [\n"
                + "          {\n"
                + "            \"ports\": [\n"
                + "              {\n"
                + "                \"containerPort\": 5701\n"
                + "              }\n"
                + "            ]\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"status\": {\n"
                + "        \"podIP\": \"192.168.0.25\",\n"
                + "        \"containerStatuses\": [\n"
                + "          {\n"
                + "            \"ready\": true\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"spec\": {\n"
                + "        \"containers\": [\n"
                + "          {\n"
                + "            \"ports\": [\n"
                + "              {\n"
                + "                \"containerPort\": 5702\n"
                + "              }\n"
                + "            ]\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"status\": {\n"
                + "        \"podIP\": \"172.17.0.5\",\n"
                + "        \"containerStatuses\": [\n"
                + "          {\n"
                + "            \"ready\": true\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"spec\": {\n"
                + "        \"containers\": [\n"
                + "          {\n"
                + "            \"ports\": [\n"
                + "              {\n"
                + "              }\n"
                + "            ]\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"status\": {\n"
                + "        \"podIP\": \"172.17.0.6\",\n"
                + "        \"containerStatuses\": [\n"
                + "          {\n"
                + "            \"ready\": false\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse);

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(format(result),
                containsInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702), notReady("172.17.0.6", null)));
    }

    @Test
    public void endpointsByNamespaceAndServiceLabel() {
        // given
        //language=JSON
        String endpointsListResponse = "{\n"
                + "  \"kind\": \"EndpointsList\",\n"
                + "  \"items\": [\n"
                + "    {\n"
                + "      \"subsets\": [\n"
                + "        {\n"
                + "          \"addresses\": [\n"
                + "            {\n"
                + "              \"ip\": \"192.168.0.25\",\n"
                + "              \"hazelcast-service-port\": 5701\n"
                + "            }\n"
                + "          ]\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"subsets\": [\n"
                + "        {\n"
                + "          \"addresses\": [\n"
                + "            {\n"
                + "              \"ip\": \"172.17.0.5\",\n"
                + "              \"hazelcast-service-port\": 5702\n"
                + "            }\n"
                + "          ],\n"
                + "          \"notReadyAddresses\": [\n"
                + "            {\n"
                + "              \"ip\": \"172.17.0.6\"\n"
                + "            }\n"
                + "          ],\n"
                + "          \"ports\": [\n"
                + "            {\n"
                + "              \"port\": 5701\n"
                + "            }\n"
                + "          ]\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}";
        String serviceLabel = "sample-service-label";
        String serviceLabelValue = "sample-service-label-value";
        Map<String, String> queryParams = singletonMap("labelSelector", String.format("%s=%s", serviceLabel, serviceLabelValue));
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), queryParams, endpointsListResponse);

        // when
        List<Endpoint> result = kubernetesClient.endpointsByServiceLabel(serviceLabel, serviceLabelValue);

        // then
        assertThat(format(result),
                containsInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702), notReady("172.17.0.6", 5701)));
    }

    @Test
    public void endpointsByNamespaceAndServiceName() {
        // given
        //language=JSON
        String endpointResponse = "{\n"
                + "  \"kind\": \"Endpoints\",\n"
                + "  \"subsets\": [\n"
                + "    {\n"
                + "      \"addresses\": [\n"
                + "        {\n"
                + "          \"ip\": \"192.168.0.25\",\n"
                + "          \"hazelcast-service-port\": 5701\n"
                + "        },\n"
                + "        {\n"
                + "          \"ip\": \"172.17.0.5\",\n"
                + "          \"hazelcast-service-port\": 5702\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}";
        String serviceName = "service-name";
        stub(String.format("/api/v1/namespaces/%s/endpoints/%s", NAMESPACE, serviceName), endpointResponse);

        // when
        List<Endpoint> result = kubernetesClient.endpointsByName(serviceName);

        // then
        assertThat(format(result), containsInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702)));
    }

    @Test
    public void endpointsByNamespaceAndPodLabel() {
        // given
        //language=JSON
        String podsListResponse = "{\n"
                + "  \"kind\": \"PodList\",\n"
                + "  \"items\": [\n"
                + "    {\n"
                + "      \"spec\": {\n"
                + "        \"containers\": [\n"
                + "          {\n"
                + "            \"ports\": [\n"
                + "              {\n"
                + "                \"containerPort\": 5701\n"
                + "              }\n"
                + "            ]\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"status\": {\n"
                + "        \"podIP\": \"192.168.0.25\",\n"
                + "        \"containerStatuses\": [\n"
                + "          {\n"
                + "            \"ready\": true\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"spec\": {\n"
                + "        \"containers\": [\n"
                + "          {\n"
                + "            \"ports\": [\n"
                + "              {\n"
                + "                \"containerPort\": 5702\n"
                + "              }\n"
                + "            ]\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"status\": {\n"
                + "        \"podIP\": \"172.17.0.5\",\n"
                + "        \"containerStatuses\": [\n"
                + "          {\n"
                + "            \"ready\": true\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String podLabel = "sample-pod-label";
        String podLabelValue = "sample-pod-label-value";
        Map<String, String> queryParams = singletonMap("labelSelector", String.format("%s=%s", podLabel, podLabelValue));
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE, podLabel), queryParams, podsListResponse);

        // when
        List<Endpoint> result = kubernetesClient.endpointsByPodLabel(podLabel, podLabelValue);

        // then
        assertThat(format(result),
                containsInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702)));
    }

    @Test
    public void zoneBeta() {
        // given
        String podName = "pod-name";

        //language=JSON
        String podResponse = "{\n"
                + "  \"kind\": \"Pod\",\n"
                + "  \"spec\": {\n"
                + "    \"nodeName\": \"node-name\"\n"
                + "  }\n"
                + "}";
        stub(String.format("/api/v1/namespaces/%s/pods/%s", NAMESPACE, podName), podResponse);

        //language=JSON
        String nodeResponse = "{\n"
                + "  \"kind\": \"Node\",\n"
                + "  \"metadata\": {\n"
                + "    \"labels\": {\n"
                + "      \"failure-domain.beta.kubernetes.io/region\": \"us-central1\",\n"
                + "      \"failure-domain.beta.kubernetes.io/zone\": \"us-central1-a\"\n"
                + "    }\n"
                + "  }\n"
                + "}";
        stub("/api/v1/nodes/node-name", nodeResponse);

        // when
        String zone = kubernetesClient.zone(podName);

        // then
        assertEquals("us-central1-a", zone);
    }

    @Test
    public void zoneFailureDomain() {
        // given
        String podName = "pod-name";

        //language=JSON
        String podResponse = "{\n"
                + "  \"kind\": \"Pod\",\n"
                + "  \"spec\": {\n"
                + "    \"nodeName\": \"node-name\"\n"
                + "  }\n"
                + "}";
        stub(String.format("/api/v1/namespaces/%s/pods/%s", NAMESPACE, podName), podResponse);

        //language=JSON
        String nodeResponse = "{\n"
                + "  \"kind\": \"Node\",\n"
                + "  \"metadata\": {\n"
                + "    \"labels\": {\n"
                + "      \"failure-domain.beta.kubernetes.io/region\": \"deprecated-region\",\n"
                + "      \"failure-domain.beta.kubernetes.io/zone\": \"deprecated-zone\",\n"
                + "      \"failure-domain.kubernetes.io/region\": \"us-central1\",\n"
                + "      \"failure-domain.kubernetes.io/zone\": \"us-central1-a\"\n"
                + "    }\n"
                + "  }\n"
                + "}";
        stub("/api/v1/nodes/node-name", nodeResponse);

        // when
        String zone = kubernetesClient.zone(podName);

        // then
        assertEquals("us-central1-a", zone);
    }


    @Test
    public void nodeName() {
        // given
        String podName = "pod-name";

        //language=JSON
        String podResponse = "{\n"
                + "  \"kind\": \"Pod\",\n"
                + "  \"spec\": {\n"
                + "    \"nodeName\": \"kubernetes-node-f0bbd602-f7cw\"\n"
                + "  }\n"
                + "}";
        stub(String.format("/api/v1/namespaces/%s/pods/%s", NAMESPACE, podName), podResponse);

        // when
        String nodeName = kubernetesClient.nodeName(podName);

        // then
        assertEquals("kubernetes-node-f0bbd602-f7cw", nodeName);
    }

    @Test
    public void zone() {
        // given
        String podName = "pod-name";

        //language=JSON
        String podResponse = "{\n"
                + "  \"kind\": \"Pod\",\n"
                + "  \"spec\": {\n"
                + "    \"nodeName\": \"node-name\"\n"
                + "  }\n"
                + "}";
        stub(String.format("/api/v1/namespaces/%s/pods/%s", NAMESPACE, podName), podResponse);

        //language=JSON
        String nodeResponse = "{\n"
                + "  \"kind\": \"Node\",\n"
                + "  \"metadata\": {\n"
                + "    \"labels\": {\n"
                + "      \"failure-domain.beta.kubernetes.io/region\": \"deprecated-region\",\n"
                + "      \"failure-domain.beta.kubernetes.io/zone\": \"deprecated-zone\",\n"
                + "      \"topology.kubernetes.io/region\": \"us-central1\",\n"
                + "      \"topology.kubernetes.io/zone\": \"us-central1-a\"\n"
                + "    }\n"
                + "  }\n"
                + "}";
        stub("/api/v1/nodes/node-name", nodeResponse);

        // when
        String zone = kubernetesClient.zone(podName);

        // then
        assertEquals("us-central1-a", zone);
    }

    @Test
    public void endpointsByNamespaceWithLoadBalancerPublicIp() {
        // given
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());

        //language=JSON
        String serviceResponse1 = "{\n"
                + "  \"kind\": \"Service\",\n"
                + "  \"spec\": {\n"
                + "    \"ports\": [\n"
                + "      {\n"
                + "        \"port\": 32123,\n"
                + "        \"targetPort\": 5701,\n"
                + "        \"nodePort\": 31916\n"
                + "      }\n"
                + "    ]\n"
                + "  },\n"
                + "  \"status\": {\n"
                + "    \"loadBalancer\": {\n"
                + "      \"ingress\": [\n"
                + "        {\n"
                + "          \"ip\": \"35.232.226.200\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        stub(String.format("/api/v1/namespaces/%s/services/service-0", NAMESPACE), serviceResponse1);

        //language=JSON
        String serviceResponse2 = "{\n"
                + "  \"kind\": \"Service\",\n"
                + "  \"spec\": {\n"
                + "    \"ports\": [\n"
                + "      {\n"
                + "        \"port\": 32124,\n"
                + "        \"targetPort\": 5701,\n"
                + "        \"nodePort\": 31916\n"
                + "      }\n"
                + "    ]\n"
                + "  },\n"
                + "  \"status\": {\n"
                + "    \"loadBalancer\": {\n"
                + "      \"ingress\": [\n"
                + "        {\n"
                + "          \"ip\": \"35.232.226.201\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  }\n"
                + "}";
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE), serviceResponse2);

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(format(result), containsInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702)));
        assertThat(formatPublic(result), containsInAnyOrder(ready("35.232.226.200", 32123), ready("35.232.226.201", 32124)));
    }

    @Test
    public void endpointsByNamespaceWithNodePublicIp() {
        // given
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/service-0", NAMESPACE), nodePortService1Response());
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE), nodePortService2Response());

        //language=JSON
        String nodeResponse1 = "{\n"
                + "  \"kind\": \"Node\",\n"
                + "  \"status\": {\n"
                + "    \"addresses\": [\n"
                + "      {\n"
                + "        \"type\": \"InternalIP\",\n"
                + "        \"address\": \"10.240.0.21\"\n"
                + "      },\n"
                + "      {\n"
                + "        \"type\": \"ExternalIP\",\n"
                + "        \"address\": \"35.232.226.200\"\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}\n";
        stub("/api/v1/nodes/node-name-1", nodeResponse1);

        String nodeResponse2 = "{\n"
                + "  \"kind\": \"Node\",\n"
                + "  \"status\": {\n"
                + "    \"addresses\": [\n"
                + "      {\n"
                + "        \"type\": \"InternalIP\",\n"
                + "        \"address\": \"10.240.0.22\"\n"
                + "      },\n"
                + "      {\n"
                + "        \"type\": \"ExternalIP\",\n"
                + "        \"address\": \"35.232.226.201\"\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}\n";
        stub("/api/v1/nodes/node-name-2", nodeResponse2);

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(format(result), containsInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702)));
        assertThat(formatPublic(result), containsInAnyOrder(ready("35.232.226.200", 31916), ready("35.232.226.201", 31917)));
    }

    @Test
    public void endpointsByNamespaceWithNodeName() {
        // given
        // create KubernetesClient with useNodeNameAsExternalAddress=true
        kubernetesClient = newKubernetesClient(true);

        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/service-0", NAMESPACE), nodePortService1Response());
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE), nodePortService2Response());

        String forbiddenBody = "\"reason\":\"Forbidden\"";
        stub("/api/v1/nodes/node-name-1", 403, forbiddenBody);
        stub("/api/v1/nodes/node-name-2", 403, forbiddenBody);

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(format(result), containsInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702)));
        assertThat(formatPublic(result), containsInAnyOrder(ready("node-name-1", 31916), ready("node-name-2", 31917)));
    }

    private static String podsListResponse() {
        //language=JSON
        return "{\n"
                + "  \"kind\": \"PodList\",\n"
                + "  \"items\": [\n"
                + "    {\n"
                + "      \"spec\": {\n"
                + "        \"containers\": [\n"
                + "          {\n"
                + "            \"ports\": [\n"
                + "              {\n"
                + "                \"containerPort\": 5701\n"
                + "              }\n"
                + "            ]\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"status\": {\n"
                + "        \"podIP\": \"192.168.0.25\",\n"
                + "        \"containerStatuses\": [\n"
                + "          {\n"
                + "            \"ready\": true\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"spec\": {\n"
                + "        \"containers\": [\n"
                + "          {\n"
                + "            \"ports\": [\n"
                + "              {\n"
                + "                \"containerPort\": 5702\n"
                + "              }\n"
                + "            ]\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"status\": {\n"
                + "        \"podIP\": \"172.17.0.5\",\n"
                + "        \"containerStatuses\": [\n"
                + "          {\n"
                + "            \"ready\": true\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";
    }

    private static String endpointsListResponse() {
        //language=JSON
        return "{\n"
                + "  \"kind\": \"EndpointsList\",\n"
                + "  \"items\": [\n"
                + "    {\n"
                + "      \"metadata\": {\n"
                + "        \"name\": \"my-release-hazelcast\"\n"
                + "      },\n"
                + "      \"subsets\": [\n"
                + "        {\n"
                + "          \"addresses\": [\n"
                + "            {\n"
                + "              \"ip\": \"192.168.0.25\",\n"
                + "              \"nodeName\": \"node-name-1\"\n"
                + "            },\n"
                + "            {\n"
                + "              \"ip\": \"172.17.0.5\",\n"
                + "              \"nodeName\": \"node-name-2\"\n"
                + "            }\n"
                + "          ],\n"
                + "          \"ports\": [\n"
                + "            {\n"
                + "              \"port\": 5701\n"
                + "            }\n"
                + "          ]\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"metadata\": {\n"
                + "        \"name\": \"service-0\"\n"
                + "      },\n"
                + "      \"subsets\": [\n"
                + "        {\n"
                + "          \"addresses\": [\n"
                + "            {\n"
                + "              \"ip\": \"192.168.0.25\",\n"
                + "              \"nodeName\": \"node-name-1\"\n"
                + "            }\n"
                + "          ],\n"
                + "          \"ports\": [\n"
                + "            {\n"
                + "              \"port\": 5701\n"
                + "            }\n"
                + "          ]\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"metadata\": {\n"
                + "        \"name\": \"service-1\"\n"
                + "      },\n"
                + "      \"subsets\": [\n"
                + "        {\n"
                + "          \"addresses\": [\n"
                + "            {\n"
                + "              \"ip\": \"172.17.0.5\",\n"
                + "              \"nodeName\": \"node-name-2\"\n"
                + "            }\n"
                + "          ],\n"
                + "          \"ports\": [\n"
                + "            {\n"
                + "              \"port\": 5702\n"
                + "            }\n"
                + "          ]\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}";
    }

    private static String nodePortService1Response() {
        //language=JSON
        return "{\n"
                + "  \"kind\": \"Service\",\n"
                + "  \"spec\": {\n"
                + "    \"ports\": [\n"
                + "      {\n"
                + "        \"port\": 32123,\n"
                + "        \"targetPort\": 5701,\n"
                + "        \"nodePort\": 31916\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}\n";
    }

    private static String nodePortService2Response() {
        //language=JSON
        return "{\n"
                + "  \"kind\": \"Service\",\n"
                + "  \"spec\": {\n"
                + "    \"ports\": [\n"
                + "      {\n"
                + "        \"port\": 32124,\n"
                + "        \"targetPort\": 5701,\n"
                + "        \"nodePort\": 31917\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}";
    }

    @Test
    public void forbidden() {
        // given
        String forbiddenBody = "\"reason\":\"Forbidden\"";
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), 403, forbiddenBody);

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertEquals(emptyList(), result);
    }

    @Test
    public void wrongApiToken() {
        // given
        String unauthorizedBody = "\"reason\":\"Unauthorized\"";
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), 401, unauthorizedBody);

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertEquals(emptyList(), result);
    }

    @Test(expected = RestClientException.class)
    public void unknownException() {
        // given
        String notRetriedErrorBody = "\"reason\":\"Forbidden\"";
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), 501, notRetriedErrorBody);

        // when
        kubernetesClient.endpoints();
    }

    private KubernetesClient newKubernetesClient(boolean useNodeNameAsExternalAddress) {
        String kubernetesMasterUrl = String.format("http://%s:%d", KUBERNETES_MASTER_IP, wireMockRule.port());
        return new KubernetesClient(NAMESPACE, kubernetesMasterUrl, TOKEN, CA_CERTIFICATE, RETRIES, useNodeNameAsExternalAddress);
    }

    private static List<String> format(List<Endpoint> addresses) {
        List<String> result = new ArrayList<String>();
        for (Endpoint address : addresses) {
            String ip = address.getPrivateAddress().getIp();
            Integer port = address.getPrivateAddress().getPort();
            boolean isReady = address.isReady();
            result.add(toString(ip, port, isReady));
        }
        return result;
    }

    private static List<String> formatPublic(List<Endpoint> addresses) {
        List<String> result = new ArrayList<String>();
        for (Endpoint address : addresses) {
            String ip = address.getPublicAddress().getIp();
            Integer port = address.getPublicAddress().getPort();
            boolean isReady = address.isReady();
            result.add(toString(ip, port, isReady));
        }
        return result;
    }

    private static void stub(String url, String response) {
        stub(url, 200, response);
    }

    private static void stub(String url, int status, String response) {
        stubFor(get(urlEqualTo(url))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(status).withBody(response)));
    }

    private static void stub(String url, Map<String, String> queryParams, String response) {
        MappingBuilder mappingBuilder = get(urlPathMatching(url));
        for (String key : queryParams.keySet()) {
            mappingBuilder = mappingBuilder.withQueryParam(key, equalTo(queryParams.get(key)));
        }
        stubFor(mappingBuilder
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(response)));
    }

    private static String ready(String ip, Integer port) {
        return toString(ip, port, true);
    }

    private static String notReady(String ip, Integer port) {
        return toString(ip, port, false);
    }

    private static String toString(String ip, Integer port, boolean isReady) {
        return String.format("%s:%s:%s", ip, port, isReady);
    }

    @Test
    @Ignore
    public void rbacYamlFileExists() {
        // rbac.yaml file is mentioned in logs, so the file must exist in the repo
        assertTrue(new File("rbac.yaml").exists());
    }
}
