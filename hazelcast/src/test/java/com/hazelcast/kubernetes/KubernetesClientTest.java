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
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.instance.impl.ClusterTopologyIntentTracker;
import com.hazelcast.kubernetes.KubernetesClient.Endpoint;
import com.hazelcast.kubernetes.KubernetesConfig.ExposeExternallyMode;
import com.hazelcast.spi.exception.RestClientException;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import org.junit.After;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
import static com.hazelcast.kubernetes.KubernetesFakeUtils.endpointPort;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.endpointSliceList;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.endpoints;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.endpointsList;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.node;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.notReadyPod;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.pod;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.podsList;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.podsListMultiplePorts;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.service;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.serviceLb;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.serviceLbHost;
import static com.hazelcast.kubernetes.KubernetesFakeUtils.servicePort;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KubernetesClientTest {
    private static final String KUBERNETES_MASTER_IP = "localhost";
    private static final String TOKEN = "sample-token";
    private static final String NAMESPACE = "sample-namespace";
    private static final int RETRIES = 3;
    private static final ObjectWriter WRITER = new ObjectMapper().writer();

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

    @After
    public void cleanUpClient() {
        if (kubernetesClient != null) {
            kubernetesClient.destroy();
        }
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
        assertThat(kubernetesClient.buildKubernetesApiUrlProvider()).isInstanceOf(KubernetesApiEndpointProvider.class);
    }

    @Test
    public void buildKubernetesApiUrlProviderReturnsEndpointSlicesProvider() throws JsonProcessingException {
        stub(String.format("/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices", NAMESPACE),
                endpointSliceList(Collections.singletonList(443), "34.68.96.71"));
        assertThat(kubernetesClient.buildKubernetesApiUrlProvider()).isInstanceOf(KubernetesApiEndpointSlicesProvider.class);
    }

    @Test
    public void endpointsByNamespace() throws JsonProcessingException {
        // given
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsList(
                pod("hazelcast-0", NAMESPACE, "node-name-1", "192.168.0.25", 5701),
                pod("hazelcast-0", NAMESPACE, "node-name-1", "172.17.0.5", 5702),
                notReadyPod("hazelcast-0", NAMESPACE, "node-name-1", "172.17.0.7")));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-0", NAMESPACE),
                pod("hazelcast-0", NAMESPACE, "node-name-1", 5701));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-1", NAMESPACE),
                pod("hazelcast-1", NAMESPACE, "node-name-2", 5701));

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702), notReady("172.17.0.7", null));
    }

    @Test
    public void endpointsByNamespaceAndServiceLabel() throws JsonProcessingException {
        // given
        String serviceLabel = "sample-service-label";
        String serviceLabelValue = "sample-service-label-value";
        Map<String, String> queryParams = singletonMap("labelSelector", String.format("%s=%s", serviceLabel, serviceLabelValue));
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), queryParams,
                endpointsList(
                        endpoints("192.168.0.25", "hazelcast-1",
                                endpointPort("some-port", 5701),
                                endpointPort("hazelcast", 5702)),
                        endpoints("172.17.0.5", "172.17.0.6", "hazelcast-1", 5701)
                ));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-0", NAMESPACE),
                pod("hazelcast-0", NAMESPACE, "node-name-1", 5701));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-1", NAMESPACE),
                pod("hazelcast-1", NAMESPACE, "node-name-2", 5701));


        // when
        List<Endpoint> result = kubernetesClient.endpointsByServiceLabel(serviceLabel, serviceLabelValue);

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5702), ready("172.17.0.5", 5701), notReady("172.17.0.6", 5701));
    }

    @Test
    public void endpointsByNamespaceAndMultipleServiceLabels() throws JsonProcessingException {
        // given
        String serviceLabels = "service-label-1,service-label-2";
        String serviceLabelValues = "service-label-value-1,service-label-value-2";
        Map<String, String> queryParams = singletonMap("labelSelector", "service-label-1=service-label-value-1,service-label-2=service-label-value-2");
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), queryParams,
                endpointsList(
                        endpoints("192.168.0.25", "hazelcast-1",
                                endpointPort("some-port", 5701),
                                endpointPort("hazelcast", 5702)),
                        endpoints("172.17.0.5", "172.17.0.6", "hazelcast-1", 5701)
                ));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-0", NAMESPACE),
                pod("hazelcast-0", NAMESPACE, "node-name-1", 5701));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-1", NAMESPACE),
                pod("hazelcast-1", NAMESPACE, "node-name-2", 5701));

        // when
        List<Endpoint> result = kubernetesClient.endpointsByServiceLabel(serviceLabels, serviceLabelValues);

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5702), ready("172.17.0.5", 5701), notReady("172.17.0.6", 5701));
    }

    @Test
    public void endpointsByNamespaceAndServiceName() throws JsonProcessingException {
        // given
        String serviceName = "service-name";
        stub(String.format("/api/v1/namespaces/%s/endpoints/%s", NAMESPACE, serviceName),
                endpoints(Arrays.asList("192.168.0.25", "172.17.0.5"), Collections.singletonList(5702)));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-0", NAMESPACE),
                pod("hazelcast-0", NAMESPACE, "node-name-1", 5701));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-1", NAMESPACE),
                pod("hazelcast-1", NAMESPACE, "node-name-2", 5701));

        // when
        List<Endpoint> result = kubernetesClient.endpointsByName(serviceName);

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5702), ready("172.17.0.5", 5702));
    }

    @Test
    public void endpointsByNamespaceAndPodLabel() throws JsonProcessingException {
        // given
        String podLabel = "sample-pod-label";
        String podLabelValue = "sample-pod-label-value";
        Map<String, String> queryParams = singletonMap("labelSelector", String.format("%s=%s", podLabel, podLabelValue));
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), queryParams, podsList(Arrays.asList(
                new KubernetesClient.EndpointAddress("192.168.0.25", 5701),
                new KubernetesClient.EndpointAddress("172.17.0.5", 5702)
        )));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-0", NAMESPACE),
                pod("hazelcast-0", NAMESPACE, "node-name-1", 5701));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-1", NAMESPACE),
                pod("hazelcast-1", NAMESPACE, "node-name-2", 5701));

        // when
        List<Endpoint> result = kubernetesClient.endpointsByPodLabel(podLabel, podLabelValue);

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702));
    }


    @Test
    public void endpointsByNamespaceAndMultiplePodLabels() throws JsonProcessingException {
        // given
        String podLabel = "pod-label-1,pod-label-2";
        String podLabelValue = "pod-label-value-1,pod-label-value-2";
        Map<String, String> queryParams = singletonMap("labelSelector", "pod-label-1=pod-label-value-1,pod-label-2=pod-label-value-2");
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), queryParams, podsList(Arrays.asList(
                new KubernetesClient.EndpointAddress("192.168.0.25", 5701),
                new KubernetesClient.EndpointAddress("172.17.0.5", 5702)
        )));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-0", NAMESPACE),
                pod("hazelcast-0", NAMESPACE, "node-name-1", 5701));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-1", NAMESPACE),
                pod("hazelcast-1", NAMESPACE, "node-name-2", 5701));

        // when
        List<Endpoint> result = kubernetesClient.endpointsByPodLabel(podLabel, podLabelValue);

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702));
    }

    @Test
    public void zoneBeta() throws JsonProcessingException {
        // given
        String podName = "pod-name";
        stub(String.format("/api/v1/namespaces/%s/pods/%s", NAMESPACE, podName), pod("hazelcast-0", NAMESPACE, "node-name"));

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
    public void zoneFailureDomain() throws JsonProcessingException {
        // given
        String podName = "pod-name";
        stub(String.format("/api/v1/namespaces/%s/pods/%s", NAMESPACE, podName), pod("hazelcast-0", NAMESPACE, "node-name"));

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
    public void nodeName() throws JsonProcessingException {
        // given
        String podName = "pod-name";
        stub(String.format("/api/v1/namespaces/%s/pods/%s", NAMESPACE, podName), pod("hazelcast-0", NAMESPACE, "kubernetes-node-f0bbd602-f7cw"));

        // when
        String nodeName = kubernetesClient.nodeName(podName);

        // then
        assertEquals("kubernetes-node-f0bbd602-f7cw", nodeName);
    }

    @Test
    public void zone() throws JsonProcessingException {
        // given
        String podName = "pod-name";
        stub(String.format("/api/v1/namespaces/%s/pods/%s", NAMESPACE, podName), pod("hazelcast-0", NAMESPACE, "node-name"));

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
    public void endpointsByNamespaceWithLoadBalancerPublicIp() throws JsonProcessingException {
        // given
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());
        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE),
                serviceLb(servicePort(32123, 5701, 31916), "35.232.226.200"));
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE),
                serviceLb(servicePort(32124, 5701, 31916), "35.232.226.201"));

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702));
        assertThat(formatPublic(result)).containsExactlyInAnyOrder(ready("35.232.226.200", 32123), ready("35.232.226.201", 32124));
    }

    @Test
    public void endpointsByNamespaceWithLoadBalancerHostname() throws JsonProcessingException {
        // given
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());
        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE),
                serviceLbHost(servicePort(32123, 5701, 31916), "abc.hostname"));
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE),
                serviceLbHost(servicePort(32124, 5701, 31916), "abc2.hostname"));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-0", NAMESPACE),
                pod("hazelcast-0", NAMESPACE, "node-name-1", 5701));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-1", NAMESPACE),
                pod("hazelcast-1", NAMESPACE, "node-name-2", 5701));


        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702));
        assertThat(formatPublic(result)).containsExactlyInAnyOrder(ready("abc.hostname", 32123), ready("abc2.hostname", 32124));
    }

    @Test
    public void endpointsByNamespaceWithNodePortPublicIp() throws JsonProcessingException {
        // given
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE), service(servicePort(32123, 5701, 31916)));
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE), service(servicePort(32124, 5701, 31917)));
        stub("/api/v1/nodes/node-name-1", node("node-name-1", "10.240.0.21", "35.232.226.200"));
        stub("/api/v1/nodes/node-name-2", node("node-name-1", "10.240.0.22", "35.232.226.201"));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-0", NAMESPACE),
                pod("hazelcast-0", NAMESPACE, "node-name-1", 5701));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-1", NAMESPACE),
                pod("hazelcast-1", NAMESPACE, "node-name-2", 5701));

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702));
        assertThat(formatPublic(result)).containsExactlyInAnyOrder(ready("35.232.226.200", 31916), ready("35.232.226.201", 31917));
    }

    @Test
    public void endpointsByNamespaceWithMultipleNodePortPublicIpMatchByName() throws JsonProcessingException {
        // given
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/service-0", NAMESPACE), service(servicePort(0, 0, 0)));
        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE),
                service(servicePort(32123, 5701, 31916)));
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE),
                service(servicePort(32124, 5701, 31917)));
        stub("/api/v1/nodes/node-name-1", node("node-name-1", "10.240.0.21", "35.232.226.200"));
        stub("/api/v1/nodes/node-name-2", node("node-name-1", "10.240.0.22", "35.232.226.201"));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-0", NAMESPACE),
                pod("hazelcast-0", NAMESPACE, "node-name-1", 5701));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-1", NAMESPACE),
                pod("hazelcast-1", NAMESPACE, "node-name-2", 5701));


        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702));
        assertThat(formatPublic(result)).containsExactlyInAnyOrder(ready("35.232.226.200", 31916), ready("35.232.226.201", 31917));
    }

    @Test
    public void endpointsByNamespaceWithMultipleNodePortPublicIpMatchByServicePerPodLabel() throws JsonProcessingException {
        // given
        String servicePerPodLabel = "sample-service-per-pod-service-label";
        String servicePerPodLabelValue = "sample-service-per-pod-service-label-value";
        cleanUpClient();
        kubernetesClient = newKubernetesClient(false, servicePerPodLabel, servicePerPodLabelValue);

        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        Map<String, String> queryParams = singletonMap("labelSelector", String.format("%s=%s", servicePerPodLabel, servicePerPodLabelValue));
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), queryParams, endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE), service(servicePort(32123, 5701, 31916)));
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE), service(servicePort(32124, 5701, 31917)));
        stub("/api/v1/nodes/node-name-1", node("node-name-1", "10.240.0.21", "35.232.226.200"));
        stub("/api/v1/nodes/node-name-2", node("node-name-1", "10.240.0.22", "35.232.226.201"));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-0", NAMESPACE),
                pod("hazelcast-0", NAMESPACE, "node-name-1", 5701));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-1", NAMESPACE),
                pod("hazelcast-1", NAMESPACE, "node-name-2", 5701));


        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702));
        assertThat(formatPublic(result)).containsExactlyInAnyOrder(ready("35.232.226.200", 31916), ready("35.232.226.201", 31917));
    }

    @Test
    public void endpointsByNamespaceWithNodeName() throws JsonProcessingException {
        // given
        // create KubernetesClient with useNodeNameAsExternalAddress=true
        cleanUpClient();
        kubernetesClient = newKubernetesClient(true);

        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE), service(servicePort(32123, 5701, 31916)));
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE), service(servicePort(32124, 5701, 31917)));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-0", NAMESPACE),
                pod("hazelcast-0", NAMESPACE, "node-name-1", 5701));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-1", NAMESPACE),
                pod("hazelcast-1", NAMESPACE, "node-name-1", 5701));


        String forbiddenBody = "\"reason\":\"Forbidden\"";
        stub("/api/v1/nodes/node-name-1", 403, forbiddenBody);
        stub("/api/v1/nodes/node-name-2", 403, forbiddenBody);

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702));
        assertThat(formatPublic(result)).containsExactlyInAnyOrder(ready("node-name-1", 31916), ready("node-name-2", 31917));
    }

    @Test
    public void endpointsIgnoreNoPublicAccess() throws JsonProcessingException {
        // given
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/service-0", NAMESPACE),
                service(servicePort(0, 0, 0), servicePort(1, 1, 2)));
        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE),
                service(servicePort(0, 0, 0), servicePort(1, 1, 2)));
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE),
                service(servicePort(0, 0, 0), servicePort(1, 1, 2)));
        stub("/api/v1/nodes/node-name-1", node("node-name-1", "10.240.0.21", "35.232.226.200"));
        stub("/api/v1/nodes/node-name-2", node("node-name-1", "10.240.0.22", "35.232.226.201"));

        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702));
    }

    @Test(expected = KubernetesClientException.class)
    public void endpointsFailFastWhenNoPublicAccess() throws JsonProcessingException {
        // given
        cleanUpClient();
        kubernetesClient = newKubernetesClient(ExposeExternallyMode.ENABLED, false, null, null);

        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/service-0", NAMESPACE),
                service(servicePort(0, 0, 0), servicePort(1, 1, 2)));
        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE),
                service(servicePort(0, 0, 0), servicePort(1, 1, 2)));
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE),
                service(servicePort(0, 0, 0), servicePort(1, 1, 2)));
        stub("/api/v1/nodes/node-name-1", node("node-name-1", "10.240.0.21", "35.232.226.200"));
        stub("/api/v1/nodes/node-name-2", node("node-name-1", "10.240.0.22", "35.232.226.201"));

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

        cleanUpClient();
        kubernetesClient = newKubernetesClient(new FileReaderTokenProvider(file.toString()));
        kubernetesClient.endpoints();
        assertFalse(kubernetesClient.isKnownExceptionAlreadyLogged());

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

        kubernetesClient.endpoints();
        assertFalse(kubernetesClient.isKnownExceptionAlreadyLogged());
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

    @Test
    public void rbacYamlFileExists() {
        // rbac.yaml file is mentioned in logs, so the file must exist in the repo
        assertTrue(new File("../kubernetes-rbac.yaml").exists());
    }

    @Test
    public void endpointsWithoutNodeName() throws JsonProcessingException {
        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE),
                serviceLb(servicePort(32124, 5701, 31916), "35.232.226.200"));
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE),
                serviceLb(servicePort(32124, 5701, 31916), "35.232.226.201"));
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), endpointsListResponseWithoutNodeName());
        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListResponse());
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-0", NAMESPACE),
                pod("hazelcast-0", NAMESPACE, "node-name-1", 5701));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-1", NAMESPACE),
                pod("hazelcast-1", NAMESPACE, "node-name-2", 5701));
        stub("/api/v1/nodes/node-name-1", node("node-name-1", "10.240.0.21", "35.232.226.200"));
        stub("/api/v1/nodes/node-name-2", node("node-name-1", "10.240.0.22", "35.232.226.201"));

        List<Endpoint> result = kubernetesClient.endpoints();

        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5702));
        assertThat(formatPublic(result)).containsExactlyInAnyOrder(ready("35.232.226.200", 32124), ready("35.232.226.201", 32124));
    }

    @Test
    public void advancedNetworkExternalIpServicePerPod() throws JsonProcessingException {
        // given
        String servicePerPodLabel = "hazelcast.com/service-per-pod";
        String servicePerPodLabelValue = "true";
        cleanUpClient();
        kubernetesClient = newKubernetesClient(false, servicePerPodLabel, servicePerPodLabelValue);

        String serviceName = "service-name";
        List<String> podsIps = Arrays.asList("192.168.0.25", "172.17.0.5");
        stub(String.format("/api/v1/namespaces/%s/endpoints/%s", NAMESPACE, serviceName), endpoints(podsIps, Arrays.asList(5701
                , 5701)));

        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), podsListMultiplePorts(podsIps));
        Map<String, String> queryParams = singletonMap("labelSelector", String.format("%s=%s", servicePerPodLabel, servicePerPodLabelValue));
        stub(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE), queryParams, endpointsListResponse());

        stub(String.format("/api/v1/namespaces/%s/services/hazelcast-0", NAMESPACE), service(servicePort(32123, 5701, 31916)));
        stub(String.format("/api/v1/namespaces/%s/services/service-1", NAMESPACE), service(servicePort(32124, 5701, 31917)));
        stub("/api/v1/nodes/node-name-1", node("node-name-1", "10.240.0.21", "35.232.226.200"));
        stub("/api/v1/nodes/node-name-2", node("node-name-1", "10.240.0.22", "35.232.226.201"));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-0", NAMESPACE),
                pod("hazelcast-0", NAMESPACE, "node-name-1", 5701));
        stub(String.format("/api/v1/namespaces/%s/pods/hazelcast-1", NAMESPACE),
                pod("hazelcast-1", NAMESPACE, "node-name-2", 5701));


        // when
        List<Endpoint> result = kubernetesClient.endpoints();

        // then
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("192.168.0.25", 5701), ready("172.17.0.5", 5701));
        assertThat(formatPublic(result)).containsExactlyInAnyOrder(ready("35.232.226.200", 31916), ready("35.232.226.201", 31917));
    }

    @Test
    public void portValuesForPodWithMultipleContainer() throws JsonProcessingException {
        PodList podList = new PodList();
        List<Pod> pods = new ArrayList<>();
        for (int i = 0; i <= 1; i++) {
            Pod pod = new PodBuilder()
                    .withMetadata(new ObjectMetaBuilder()
                            .withName("hazelcast-" + i)
                            .build())
                    .withSpec(new PodSpecBuilder()
                            .withContainers(new ContainerBuilder()
                                            .withName("hazelcast")
                                            .withPorts(new ContainerPortBuilder()
                                                    .withContainerPort(5701)
                                                    .withName("hazelcast")
                                                    .build())
                                            .build(),
                                    new ContainerBuilder()
                                            .withName("proxy")
                                            .withPorts(new ContainerPortBuilder()
                                                    .withContainerPort(5701)
                                                    .withName("proxy")
                                                    .build())
                                            .build())
                            .build())
                    .withStatus(new PodStatusBuilder()
                            .withContainerStatuses(new ContainerStatusBuilder().withReady().build())
                            .withPodIP(String.format("172.17.%d.5", i))
                            .build())
                    .build();
            pods.add(pod);
        }
        podList.setItems(pods);
        ObjectMapper mapper = new ObjectMapper();
        String prodsJsonResponse = mapper.writeValueAsString(podList);

        stub(String.format("/api/v1/namespaces/%s/pods", NAMESPACE), prodsJsonResponse);
        List<Endpoint> result = kubernetesClient.endpoints();
        assertThat(formatPrivate(result)).containsExactlyInAnyOrder(ready("172.17.0.5", 5701), ready("172.17.1.5", 5701));
    }

    private static PodList podsListResponse() throws JsonProcessingException {
        return podsList(
                Arrays.asList(new KubernetesClient.EndpointAddress("192.168.0.25", 5701),
                        new KubernetesClient.EndpointAddress("172.17.0.5", 5702)));
    }

    private static EndpointsList endpointsListResponse() {
        return endpointsList(
                endpoints("my-release-hazelcast", new HashMap<>() {{
                    put("192.168.0.25", "node-name-1");
                    put("172.17.0.5", "node-name-2");
                }}),
                endpoints("service-0", new HashMap<>() {{
                    put("192.168.0.25", "node-name-1");
                }}),
                endpoints("hazelcast-0", new HashMap<>() {{
                    put("192.168.0.25", "node-name-1");
                }}),
                endpoints("service-1", new HashMap<>() {{
                    put("172.17.0.5", "node-name-2");
                }}, Collections.singletonList(5702))
        );
    }

    private static String endpointsListResponseWithoutNodeName() throws JsonProcessingException {
        return WRITER.writeValueAsString(endpointsList(
                endpoints("my-release-hazelcast", new HashMap<>() {{
                    put("172.17.0.5", null);
                    put("192.168.0.25", null);
                }}),
                endpoints("service-0", new HashMap<>() {{
                    put("192.168.0.25", null);
                }}),
                endpoints("hazelcast-0", new HashMap<>() {{
                    put("192.168.0.25", null);
                }}),
                endpoints("service-1", new HashMap<>() {{
                    put("172.17.0.5", null);
                }}, Collections.singletonList(5702))
        ));
    }

    private KubernetesClient newKubernetesClient() {
        return newKubernetesClient(false);
    }

    private KubernetesClient newKubernetesClient(KubernetesTokenProvider tokenProvider) {
        String kubernetesMasterUrl = String.format("http://%s:%d", KUBERNETES_MASTER_IP, wireMockRule.port());
        return new KubernetesClient(NAMESPACE, kubernetesMasterUrl, tokenProvider, null, RETRIES,
                ExposeExternallyMode.AUTO, true, null, null,
                (ClusterTopologyIntentTracker) null);
    }

    private KubernetesClient newKubernetesClient(boolean useNodeNameAsExternalAddress) {
        return newKubernetesClient(useNodeNameAsExternalAddress, null, null);
    }

    private KubernetesClient newKubernetesClient(boolean useNodeNameAsExternalAddress, String servicePerPodLabelName,
                                                 String servicePerPodLabelValue) {
        return newKubernetesClient(ExposeExternallyMode.AUTO, useNodeNameAsExternalAddress, servicePerPodLabelName,
                servicePerPodLabelValue);
    }

    private KubernetesClient newKubernetesClient(ExposeExternallyMode exposeExternally, boolean useNodeNameAsExternalAddress,
                                                 String servicePerPodLabelName, String servicePerPodLabelValue) {
        return newKubernetesClient(exposeExternally, useNodeNameAsExternalAddress, servicePerPodLabelName,
                servicePerPodLabelValue, new KubernetesApiEndpointProvider());
    }
    private KubernetesClient newKubernetesClient(ExposeExternallyMode exposeExternally, boolean useNodeNameAsExternalAddress,
                                                 String servicePerPodLabelName, String servicePerPodLabelValue,
                                                 KubernetesApiProvider urlProvider) {
        String kubernetesMasterUrl = String.format("http://%s:%d", KUBERNETES_MASTER_IP, wireMockRule.port());
        return new KubernetesClient(NAMESPACE, kubernetesMasterUrl, new StaticTokenProvider(TOKEN), null, RETRIES,
                exposeExternally, useNodeNameAsExternalAddress, servicePerPodLabelName, servicePerPodLabelValue, urlProvider);
    }

    private static List<String> formatPrivate(List<Endpoint> addresses) {
        List<String> result = new ArrayList<>();
        for (Endpoint address : addresses) {
            String ip = address.getPrivateAddress().getIp();
            Integer port = address.getPrivateAddress().getPort();
            boolean isReady = address.isReady();
            result.add(toString(ip, port, isReady));
        }
        return result;
    }

    private static List<String> formatPublic(List<Endpoint> addresses) {
        List<String> result = new ArrayList<>();
        for (Endpoint address : addresses) {
            String ip = address.getPublicAddress().getIp();
            Integer port = address.getPublicAddress().getPort();
            boolean isReady = address.isReady();
            result.add(toString(ip, port, isReady));
        }
        return result;
    }

    private static void stub(String url, KubernetesResource response) throws JsonProcessingException {
        stub(url, 200, WRITER.writeValueAsString(response));
    }

    private static void stub(String url, String response) {
        stub(url, 200, response);
    }

    private static void stub(String url, int status, String response) {
        stubFor(get(urlEqualTo(url))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(status).withBody(response)));
    }

    private static void stub(String url, Map<String, String> queryParams, KubernetesResource response) throws JsonProcessingException {
        stub(url, queryParams, WRITER.writeValueAsString(response));
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
}
