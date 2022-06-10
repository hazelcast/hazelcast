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

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.kubernetes.KubernetesClient.Endpoint;
import com.hazelcast.kubernetes.KubernetesConfig.ExposeExternallyMode;
import com.hazelcast.spi.exception.RestClientException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KubernetesClientTest {
    private static final String KUBERNETES_MASTER_IP = "localhost";
    private static final String TOKEN = "sample-token";
    private static final String CA_CERTIFICATE = "sample-ca-certificate";
    private static final String NAMESPACE = "sample-namespace";
    private static final int RETRIES = 3;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private KubernetesClient kubernetesClient;

    @Before
    public void setUp() {
        kubernetesClient = newKubernetesClient();
        stubFor(get(urlMatching("/api/.*")).atPriority(5)
                .willReturn(aResponse().withStatus(401).withBody("\"reason\":\"Forbidden\"")));
    }

    @Test
    public void buildKubernetesApiUrlProviderReturnsEndpointProvider() {
        //language=JSON
        String endpointSlicesResponse = "{\n"
                + "  \"kind\": \"Status\",\n"
                + "  \"apiVersion\": \"v1\",\n"
                + "  \"metadata\": {\n"
                + "    \n"
                + "  },\n"
                + "  \"status\": \"Failure\",\n"
                + "  \"message\": \"the server could not find the requested resource\",\n"
                + "  \"reason\":\"NotFound\",\n"
                + "  \"details\": {\n"
                + "    \n"
                + "  },\n"
                + "  \"code\": 404\n"
                + "}";
        stub(String.format("/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices", NAMESPACE),
                404, endpointSlicesResponse);
        assertThat(kubernetesClient.buildKubernetesApiUrlProvider(), instanceOf(KubernetesApiEndpointProvider.class));
    }

    @Test
    public void buildKubernetesApiUrlProviderReturnsEndpointSlicesProvider() {
        //language=JSON
        String endpointSlicesResponse = "{\n"
                + "  \"kind\": \"EndpointSliceList\",\n"
                + "  \"apiVersion\": \"discovery.k8s.io/v1\",\n"
                + "  \"items\": [\n"
                + "    {\n"
                + "      \"metadata\": {\n"
                + "        \"name\": \"es1\",\n"
                + "        \"namespace\": \"\",\n"
                + "        \"uid\": \"someUuid\",\n"
                + "        \"labels\": {\n"
                + "          \"kubernetes.io/service-name\": \"\"\n"
                + "        }\n"
                + "      },\n"
                + "      \"addressType\": \"\",\n"
                + "      \"endpoints\": [\n"
                + "        {\n"
                + "          \"addresses\": [\n"
                + "            \"34.68.96.71\"\n"
                + "          ],\n"
                + "          \"conditions\": {\n"
                + "            \"ready\": true\n"
                + "          }\n"
                + "        }\n"
                + "      ],\n"
                + "      \"ports\": [\n"
                + "        {\n"
                + "          \"name\": \"https\",\n"
                + "          \"protocol\": \"TCP\",\n"
                + "          \"port\": 443\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}";
        stub(String.format("/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices", NAMESPACE), endpointSlicesResponse);
        assertThat(kubernetesClient.buildKubernetesApiUrlProvider(), instanceOf(KubernetesApiEndpointSlicesProvider.class));
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
               + "              \"ip\": \"192.168.0.25\"\n"
               + "            }\n"
               + "          ],\n"
               + "          \"ports\": [\n"
               + "            {\n"
               + "              \"port\": 5701\n"
               + "            },\n"
               + "            {\n"
               + "              \"name\": \"hazelcast-service-port\",\n"
               + "              \"protocol\": \"TCP\",\n"
               + "              \"port\": 5702\n"
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
               + "              \"ip\": \"172.17.0.5\"\n"
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
                containsInAnyOrder(ready("192.168.0.25", 5702), ready("172.17.0.5", 5701), notReady("172.17.0.6", 5701)));
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
              + "          \"ip\": \"192.168.0.25\"\n"
              + "        },\n"
              + "        {\n"
              + "          \"ip\": \"172.17.0.5\"\n"
              + "        }\n"
              + "      ],\n"
              + "      \"ports\": [\n"
              + "        {\n"
              + "          \"name\": \"hazelcast-service-port\",\n"
              + "          \"protocol\": \"TCP\",\n"
              + "          \"port\": 5702\n"
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
        assertThat(format(result), containsInAnyOrder(ready("192.168.0.25", 5702), ready("172.17.0.5", 5702)));
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
        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE), serviceResponse1);

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
    public void endpointsByNamespaceWithLoadBalancerHostname() {
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
                + "          \"hostname\": \"abc.hostname\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE), serviceResponse1);

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
                + "          \"hostname\": \"abc2.hostname\"\n"
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
        assertThat(formatPublic(result), containsInAnyOrder(ready("abc.hostname", 32123), ready("abc2.hostname", 32124)));
    }

    @Test
    public void endpointsByNamespaceWithNodePortPublicIp() {
        // given
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE), nodePortService1Response());
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE), nodePortService2Response());
        stub("/api/v1/nodes/node-name-1", node1Response());
        stub("/api/v1/nodes/node-name-2", node2Response());

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(format(result), containsInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702)));
        assertThat(formatPublic(result), containsInAnyOrder(ready("35.232.226.200", 31916), ready("35.232.226.201", 31917)));
    }

    @Test
    public void endpointsByNamespaceWithMultipleNodePortPublicIpMatchByName() {
        // given
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/service-0", NAMESPACE), nodePortServiceIncorrectResponse());
        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE), nodePortService1Response());
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE), nodePortService2Response());
        stub("/api/v1/nodes/node-name-1", node1Response());
        stub("/api/v1/nodes/node-name-2", node2Response());

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(format(result), containsInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702)));
        assertThat(formatPublic(result), containsInAnyOrder(ready("35.232.226.200", 31916), ready("35.232.226.201", 31917)));
    }

    @Test
    public void endpointsByNamespaceWithMultipleNodePortPublicIpMatchByServicePerPodLabel() {
        // given
        String servicePerPodLabel = "sample-service-per-pod-service-label";
        String servicePerPodLabelValue = "sample-service-per-pod-service-label-value";
        kubernetesClient = newKubernetesClient(false, servicePerPodLabel, servicePerPodLabelValue);

        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        Map<String, String> queryParams = singletonMap("labelSelector", String.format("%s=%s", servicePerPodLabel, servicePerPodLabelValue));
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), queryParams, endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE), nodePortService1Response());
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE), nodePortService2Response());
        stub("/api/v1/nodes/node-name-1", node1Response());
        stub("/api/v1/nodes/node-name-2", node2Response());

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

        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE), nodePortService1Response());
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

    @Test
    public void endpointsIgnoreNoPublicAccess() {
        // given
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/service-0", NAMESPACE), nodePortServiceIncorrectResponseException());
        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE), nodePortServiceIncorrectResponseException());
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE), nodePortServiceIncorrectResponseException());
        stub("/api/v1/nodes/node-name-1", node1Response());
        stub("/api/v1/nodes/node-name-2", node2Response());

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(format(result), containsInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702)));
    }

    @Test(expected = KubernetesClientException.class)
    public void endpointsFailFastWhenNoPublicAccess() {
        // given
        kubernetesClient = newKubernetesClient(ExposeExternallyMode.ENABLED, false, null, null);

        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/service-0", NAMESPACE), nodePortServiceIncorrectResponseException());
        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE), nodePortServiceIncorrectResponseException());
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE), nodePortServiceIncorrectResponseException());
        stub("/api/v1/nodes/node-name-1", node1Response());
        stub("/api/v1/nodes/node-name-2", node2Response());

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        // exception
    }

    @Test
    public void apiAccessWithTokenRefresh() throws IOException {
        // Token is read from the file, first token value is value-1
        File file = testFolder.newFile("token");
        Files.write(file.toPath(), "value-1".getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING);

        stubFor(get(urlMatching("/apis/.*")).atPriority(1)
                .withHeader("Authorization", equalTo("Bearer value-1"))
                .willReturn(aResponse().withStatus(200).withBody("{}")));
        stubFor(get(urlMatching("/api/.*")).atPriority(1)
                .withHeader("Authorization", equalTo("Bearer value-1"))
                .willReturn(aResponse().withStatus(200).withBody("{}")));
        stubFor(get(urlMatching("/api/.*")).atPriority(1)
                .withHeader("Authorization", equalTo("Bearer value-2"))
                .willReturn(aResponse().withStatus(402).withBody("{}")));
        stubFor(get(urlMatching("/apis/.*")).atPriority(1)
                .withHeader("Authorization", equalTo("Bearer value-2"))
                .willReturn(aResponse().withStatus(402).withBody("{}")));

        KubernetesClient client = newKubernetesClient(new FileReaderTokenProvider(file.toString()));
        client.endpoints();
        assertFalse(client.isKnownExceptionAlreadyLogged());

        // Token is rotated
        Files.write(file.toPath(), "value-2".getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING);

        // Api server will not accept token with old value
        stubFor(get(urlMatching("/apis/.*")).atPriority(1)
                .withHeader("Authorization", equalTo("Bearer value-1"))
                .willReturn(aResponse().withStatus(402).withBody("{}")));
        stubFor(get(urlMatching("/api/.*")).atPriority(1)
                .withHeader("Authorization", equalTo("Bearer value-1"))
                .willReturn(aResponse().withStatus(402).withBody("{}")));
        stubFor(get(urlMatching("/api/.*")).atPriority(1)
                .withHeader("Authorization", equalTo("Bearer value-2"))
                .willReturn(aResponse().withStatus(200).withBody("{}")));
        stubFor(get(urlMatching("/apis/.*")).atPriority(1)
                .withHeader("Authorization", equalTo("Bearer value-2"))
                .willReturn(aResponse().withStatus(200).withBody("{}")));

        client.endpoints();
        assertFalse(client.isKnownExceptionAlreadyLogged());
    }

    private static String podsListResponse() {
        //language=JSON
        return "{\n"
                + "  \"kind\": \"PodList\",\n"
                + "  \"items\": [\n"
                + "    {\n"
                + "      \"metadata\" : {\n"
                + "        \"name\" : \"hazelcast-0\"\n"
                + "      "
                + "},\n"
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
                + "      \"metadata\" : {\n"
                + "        \"name\" : \"hazelcast-1\"\n"
                + "      },\n"
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
                + "        \"name\": \"hazelcast-0\"\n"
                + "      },\n"
                + "      \"subsets\": [\n"
                + "        {\n"
                + "          \"addresses\": [\n"
                + "            {\n"
                + "              \"ip\": \"192.168.0.25\",\n"
                + "              \"nodeName\": \"node-name-1\",\n"
                + "              \"targetRef\" : {\n"
                + "                \"name\" : \"hazelcast-0\"\n"
                + "              }\n"
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


    private String nodePortServiceIncorrectResponse() {
        //language=JSON
        return "{\n"
                + "  \"kind\": \"Service\",\n"
                + "  \"spec\": {\n"
                + "    \"ports\": [\n"
                + "      {\n"
                + "        \"port\": 0,\n"
                + "        \"targetPort\": 0,\n"
                + "        \"nodePort\": 0\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}";
    }

    private String nodePortServiceIncorrectResponseException() {
        //language=JSON
        return "{\n"
                + "  \"kind\": \"Service\",\n"
                + "  \"metadata\": {\n"
                + "    \"name\": \"incorrect-service\"\n"
                + "  "
                + "},\n"
                + "  \"spec\": {\n"
                + "    \"ports\": [\n"
                + "      {\n"
                + "        \"port\": 0,\n"
                + "        \"targetPort\": 0,\n"
                + "        \"nodePort\": 0\n"
                + "      },\n"
                + "      {\n"
                + "        \"port\": 1,\n"
                + "        \"targetPort\": 1,\n"
                + "        \"nodePort\": 2\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}";
    }

    private String node1Response() {
        //language=JSON
        return "{\n"
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
    }

    private String node2Response() {
        //language=JSON
        return "{\n"
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

    @Test
    public void unknownException() {
        // given
        String notRetriedErrorBody = "\"reason\":\"Forbidden\"";
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), 501, notRetriedErrorBody);

        // when
        assertThatThrownBy(() -> kubernetesClient.endpoints())
                .isInstanceOf(RestClientException.class)
                .hasMessageContaining("Message: \"reason\":\"Forbidden\". HTTP Error Code: 501");
    }

    private KubernetesClient newKubernetesClient() {
        return newKubernetesClient(false);
    }

    private KubernetesClient newKubernetesClient(KubernetesTokenProvider tokenProvider) {
        String kubernetesMasterUrl = String.format("http://%s:%d", KUBERNETES_MASTER_IP, wireMockRule.port());
        return new KubernetesClient(NAMESPACE, kubernetesMasterUrl, tokenProvider, CA_CERTIFICATE, RETRIES, ExposeExternallyMode.AUTO, true, null, null);
    }

    private KubernetesClient newKubernetesClient(boolean useNodeNameAsExternalAddress) {
        return newKubernetesClient(useNodeNameAsExternalAddress, null, null);
    }

    private KubernetesClient newKubernetesClient(boolean useNodeNameAsExternalAddress, String servicePerPodLabelName, String servicePerPodLabelValue) {
        return newKubernetesClient(ExposeExternallyMode.AUTO, useNodeNameAsExternalAddress, servicePerPodLabelName, servicePerPodLabelValue);
    }

    private KubernetesClient newKubernetesClient(ExposeExternallyMode exposeExternally, boolean useNodeNameAsExternalAddress, String servicePerPodLabelName, String servicePerPodLabelValue) {
        return newKubernetesClient(exposeExternally, useNodeNameAsExternalAddress, servicePerPodLabelName, servicePerPodLabelValue, new KubernetesApiEndpointProvider());
    }

    private KubernetesClient newKubernetesClient(ExposeExternallyMode exposeExternally, boolean useNodeNameAsExternalAddress, String servicePerPodLabelName, String servicePerPodLabelValue, KubernetesApiProvider urlProvider) {
        String kubernetesMasterUrl = String.format("http://%s:%d", KUBERNETES_MASTER_IP, wireMockRule.port());
        return new KubernetesClient(NAMESPACE, kubernetesMasterUrl, new StaticTokenProvider(TOKEN), CA_CERTIFICATE, RETRIES, exposeExternally, useNodeNameAsExternalAddress, servicePerPodLabelName, servicePerPodLabelValue, urlProvider);
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
    public void rbacYamlFileExists() {
        // rbac.yaml file is mentioned in logs, so the file must exist in the repo
        assertTrue(new File("../kubernetes-rbac.yaml").exists());
    }
}
