/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.kubernetes.KubernetesClient.Endpoints;
import com.hazelcast.kubernetes.KubernetesClient.EntrypointAddress;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultKubernetesClientTest {
    private static final String KUBERNETES_MASTER_IP = "localhost";

    private static final String TOKEN = "sample-token";
    private static final String CA_CERTIFICATE = "sample-ca-certificate";
    private static final String NAMESPACE = "sample-namespace";

    private static final String SAMPLE_ADDRESS_1 = "192.168.0.25";
    private static final String SAMPLE_ADDRESS_2 = "172.17.0.5";
    private static final String SAMPLE_NOT_READY_ADDRESS = "172.17.0.6";
    private static final Integer SAMPLE_PORT_1 = 5701;
    private static final Integer SAMPLE_PORT_2 = 5702;
    private static final String SAMPLE_IP_PORT_1 = ipPort(SAMPLE_ADDRESS_1, SAMPLE_PORT_1);
    private static final String SAMPLE_IP_PORT_2 = ipPort(SAMPLE_ADDRESS_2, SAMPLE_PORT_2);
    private static final String SAMPLE_NOT_READY_IP = ipPort(SAMPLE_NOT_READY_ADDRESS, null);
    private static final String ZONE = "us-central1-a";

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    private DefaultKubernetesClient kubernetesClient;

    @Before
    public void setUp() {
        String kubernetesMasterUrl = String.format("http://%s:%d", KUBERNETES_MASTER_IP, wireMockRule.port());
        kubernetesClient = new DefaultKubernetesClient(kubernetesMasterUrl, TOKEN, CA_CERTIFICATE);
    }

    @Test
    public void endpointsByNamespace() {
        // given
        stubFor(get(urlEqualTo(String.format("/api/v1/namespaces/%s/pods", NAMESPACE)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(podsListBody())));

        // when
        Endpoints result = kubernetesClient.endpoints(NAMESPACE);

        // then
        assertThat(extractIpPort(result.getAddresses()), containsInAnyOrder(SAMPLE_IP_PORT_1, SAMPLE_IP_PORT_2));
        assertThat(extractIpPort(result.getNotReadyAddresses()), containsInAnyOrder(SAMPLE_NOT_READY_IP));
    }

    @Test
    public void endpointsByNamespaceAndLabel() {
        // given
        String serviceLabel = "sample-service-label";
        String serviceLabelValue = "sample-service-label-value";
        stubFor(get(urlPathMatching(String.format("/api/v1/namespaces/%s/endpoints", NAMESPACE)))
                .withQueryParam("labelSelector", equalTo(String.format("%s=%s", serviceLabel, serviceLabelValue)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(endpointsListBody())));

        // when
        Endpoints result = kubernetesClient.endpointsByLabel(NAMESPACE, serviceLabel, serviceLabelValue);

        // then
        assertThat(extractIpPort(result.getAddresses()), containsInAnyOrder(SAMPLE_IP_PORT_1, SAMPLE_IP_PORT_2));
        assertThat(extractIpPort(result.getNotReadyAddresses()), containsInAnyOrder(SAMPLE_NOT_READY_IP));

    }

    @Test
    public void endpointsByNamespaceAndServiceName() {
        // given
        String serviceName = "service-name";
        stubFor(get(urlPathMatching(String.format("/api/v1/namespaces/%s/endpoints/%s", NAMESPACE, serviceName)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(endpointsBody())));

        // when
        Endpoints result = kubernetesClient.endpointsByName(NAMESPACE, serviceName);

        // then
        assertThat(extractIpPort(result.getAddresses()), containsInAnyOrder(SAMPLE_IP_PORT_1, SAMPLE_IP_PORT_2));
        assertTrue(result.getNotReadyAddresses().isEmpty());
    }

    @Test
    public void zoneBeta() {
        // given
        String podName = "my-release-hazelcast-0";
        String nodeName = "gke-rafal-test-cluster-default-pool-9238654c-12tz";
        stubFor(get(urlPathMatching(String.format("/api/v1/namespaces/%s/pods/%s", NAMESPACE, podName)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(podBody(nodeName))));
        stubFor(get(urlPathMatching(String.format("/api/v1/nodes/%s", nodeName)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(nodeBetaBody())));

        // when
        String zone = kubernetesClient.zone(NAMESPACE, podName);

        // then
        assertEquals(ZONE, zone);
    }

    @Test
    public void zone() {
        // given
        String podName = "my-release-hazelcast-0";
        String nodeName = "gke-rafal-test-cluster-default-pool-9238654c-12tz";
        stubFor(get(urlPathMatching(String.format("/api/v1/namespaces/%s/pods/%s", NAMESPACE, podName)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(podBody(nodeName))));
        stubFor(get(urlPathMatching(String.format("/api/v1/nodes/%s", nodeName)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(nodeBody())));

        // when
        String zone = kubernetesClient.zone(NAMESPACE, podName);

        // then
        assertEquals(ZONE, zone);
    }

    @Test(expected = KubernetesClientException.class)
    public void forbidden() {
        // given
        stubFor(get(urlEqualTo(String.format("/api/v1/namespaces/%s/pods", NAMESPACE)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(501).withBody(forbiddenBody())));

        // when
        kubernetesClient.endpoints(NAMESPACE);
    }

    @Test(expected = KubernetesClientException.class)
    public void malformedResponse() {
        // given
        stubFor(get(urlEqualTo(String.format("/api/v1/namespaces/%s/pods", NAMESPACE)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(malformedBody())));

        // when
        kubernetesClient.endpoints(NAMESPACE);
    }

    @Test(expected = KubernetesClientException.class)
    public void nullToken() {
        // given
        String kubernetesMasterUrl = String.format("http://%s:%d", KUBERNETES_MASTER_IP, wireMockRule.port());
        KubernetesClient kubernetesClient = new DefaultKubernetesClient(kubernetesMasterUrl, TOKEN, null);

        stubFor(get(urlEqualTo(String.format("/api/v1/namespaces/%s/pods", NAMESPACE)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(malformedBody())));

        // when
        kubernetesClient.endpoints(NAMESPACE);
    }

    /**
     * Real response recorded from the Kubernetes API call "/api/v1/namespaces/{namespace}/pods".
     */
    private static String podsListBody() {
        return String.format("{\n"
                + "  \"kind\": \"PodList\",\n"
                + "  \"apiVersion\": \"v1\",\n"
                + "  \"metadata\": {\n"
                + "    \"selfLink\": \"/api/v1/namespaces/default/pods\",\n"
                + "    \"resourceVersion\": \"4400\"\n"
                + "  },\n"
                + "  \"items\": [\n"
                + "    {\n"
                + "      \"metadata\": {\n"
                + "        \"name\": \"my-release-hazelcast-0\",\n"
                + "        \"generateName\": \"my-release-hazelcast-\",\n"
                + "        \"namespace\": \"default\",\n"
                + "        \"selfLink\": \"/api/v1/namespaces/default/pods/my-release-hazelcast-0\",\n"
                + "        \"uid\": \"21b91e5b-eefd-11e8-ab27-42010a8001ce\",\n"
                + "        \"resourceVersion\": \"1967\",\n"
                + "        \"creationTimestamp\": \"2018-11-23T08:52:39Z\",\n"
                + "        \"labels\": {\n"
                + "          \"app\": \"hazelcast\",\n"
                + "          \"controller-revision-hash\": \"my-release-hazelcast-7bcf66dc79\",\n"
                + "          \"release\": \"my-release\",\n"
                + "          \"role\": \"hazelcast\",\n"
                + "          \"statefulset.kubernetes.io/pod-name\": \"my-release-hazelcast-0\"\n"
                + "        },\n"
                + "        \"annotations\": {\n"
                + "          \"kubernetes.io/limit-ranger\": \"LimitRanger plugin set: cpu request for container my-release-hazelcast\"\n"
                + "        },\n"
                + "        \"ownerReferences\": [\n"
                + "          {\n"
                + "            \"apiVersion\": \"apps/v1beta1\",\n"
                + "            \"kind\": \"StatefulSet\",\n"
                + "            \"name\": \"my-release-hazelcast\",\n"
                + "            \"uid\": \"21b3fb7f-eefd-11e8-ab27-42010a8001ce\",\n"
                + "            \"controller\": true,\n"
                + "            \"blockOwnerDeletion\": true\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"spec\": {\n"
                + "        \"volumes\": [\n"
                + "          {\n"
                + "            \"name\": \"hazelcast-storage\",\n"
                + "            \"configMap\": {\n"
                + "              \"name\": \"my-release-hazelcast-configuration\",\n"
                + "              \"defaultMode\": 420\n"
                + "            }\n"
                + "          },\n"
                + "          {\n"
                + "            \"name\": \"my-release-hazelcast-token-j9db4\",\n"
                + "            \"secret\": {\n"
                + "              \"secretName\": \"my-release-hazelcast-token-j9db4\",\n"
                + "              \"defaultMode\": 420\n"
                + "            }\n"
                + "          }\n"
                + "        ],\n"
                + "        \"containers\": [\n"
                + "          {\n"
                + "            \"name\": \"my-release-hazelcast\",\n"
                + "            \"image\": \"hazelcast/hazelcast:latest\",\n"
                + "            \"ports\": [\n"
                + "              {\n"
                + "                \"name\": \"hazelcast\",\n"
                + "                \"containerPort\": %s,\n"
                + "                \"protocol\": \"TCP\"\n"
                + "              }\n"
                + "            ],\n"
                + "            \"resources\": {\n"
                + "              \"requests\": {\n"
                + "                \"cpu\": \"100m\"\n"
                + "              }\n"
                + "            },\n"
                + "            \"volumeMounts\": [\n"
                + "              {\n"
                + "                \"name\": \"hazelcast-storage\",\n"
                + "                \"mountPath\": \"/data/hazelcast\"\n"
                + "              },\n"
                + "              {\n"
                + "                \"name\": \"my-release-hazelcast-token-j9db4\",\n"
                + "                \"readOnly\": true,\n"
                + "                \"mountPath\": \"/var/run/secrets/kubernetes.io/serviceaccount\"\n"
                + "              }\n"
                + "            ],\n"
                + "            \"terminationMessagePath\": \"/dev/termination-log\",\n"
                + "            \"terminationMessagePolicy\": \"File\",\n"
                + "            \"imagePullPolicy\": \"Always\"\n"
                + "          }\n"
                + "        ],\n"
                + "        \"restartPolicy\": \"Always\",\n"
                + "        \"terminationGracePeriodSeconds\": 30,\n"
                + "        \"dnsPolicy\": \"ClusterFirst\",\n"
                + "        \"serviceAccountName\": \"my-release-hazelcast\",\n"
                + "        \"serviceAccount\": \"my-release-hazelcast\",\n"
                + "        \"nodeName\": \"gke-rafal-test-cluster-default-pool-e5fb2ea5-c7g8\",\n"
                + "        \"securityContext\": {\n"
                + "\n"
                + "        },\n"
                + "        \"hostname\": \"my-release-hazelcast-0\",\n"
                + "        \"schedulerName\": \"default-scheduler\",\n"
                + "        \"tolerations\": [\n"
                + "          {\n"
                + "            \"key\": \"node.kubernetes.io/not-ready\",\n"
                + "            \"operator\": \"Exists\",\n"
                + "            \"effect\": \"NoExecute\",\n"
                + "            \"tolerationSeconds\": 300\n"
                + "          },\n"
                + "          {\n"
                + "            \"key\": \"node.kubernetes.io/unreachable\",\n"
                + "            \"operator\": \"Exists\",\n"
                + "            \"effect\": \"NoExecute\",\n"
                + "            \"tolerationSeconds\": 300\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"status\": {\n"
                + "        \"phase\": \"Running\",\n"
                + "        \"conditions\": [\n"
                + "          {\n"
                + "            \"type\": \"Initialized\",\n"
                + "            \"status\": \"True\",\n"
                + "            \"lastProbeTime\": null,\n"
                + "            \"lastTransitionTime\": \"2018-11-23T08:52:39Z\"\n"
                + "          },\n"
                + "          {\n"
                + "            \"type\": \"Ready\",\n"
                + "            \"status\": \"True\",\n"
                + "            \"lastProbeTime\": null,\n"
                + "            \"lastTransitionTime\": \"2018-11-23T08:53:21Z\"\n"
                + "          },\n"
                + "          {\n"
                + "            \"type\": \"PodScheduled\",\n"
                + "            \"status\": \"True\",\n"
                + "            \"lastProbeTime\": null,\n"
                + "            \"lastTransitionTime\": \"2018-11-23T08:52:39Z\"\n"
                + "          }\n"
                + "        ],\n"
                + "        \"hostIP\": \"10.240.0.18\",\n"
                + "        \"podIP\": \"%s\",\n"
                + "        \"startTime\": \"2018-11-23T08:52:39Z\",\n"
                + "        \"containerStatuses\": [\n"
                + "          {\n"
                + "            \"name\": \"my-release-hazelcast\",\n"
                + "            \"state\": {\n"
                + "              \"running\": {\n"
                + "                \"startedAt\": \"2018-11-23T08:52:47Z\"\n"
                + "              }\n"
                + "            },\n"
                + "            \"lastState\": {\n"
                + "\n"
                + "            },\n"
                + "            \"ready\": true,\n"
                + "            \"restartCount\": 0,\n"
                + "            \"image\": \"hazelcast/hazelcast:latest\",\n"
                + "            \"imageID\": \"docker-pullable://hazelcast/hazelcast@sha256:a4dd478dc792ba3fa560aa41b107fed676b37c283be0306303544a0a8ebcc4c8\",\n"
                + "            \"containerID\": \"docker://d2c59dd02561ae2d274dfa0413277422383241425ce5701ca36c30a862d1520a\"\n"
                + "          }\n"
                + "        ],\n"
                + "        \"qosClass\": \"Burstable\"\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"metadata\": {\n"
                + "        \"name\": \"my-release-hazelcast-1\",\n"
                + "        \"generateName\": \"my-release-hazelcast-\",\n"
                + "        \"namespace\": \"default\",\n"
                + "        \"selfLink\": \"/api/v1/namespaces/default/pods/my-release-hazelcast-1\",\n"
                + "        \"uid\": \"3a7fd73f-eefd-11e8-ab27-42010a8001ce\",\n"
                + "        \"resourceVersion\": \"2022\",\n"
                + "        \"creationTimestamp\": \"2018-11-23T08:53:21Z\",\n"
                + "        \"labels\": {\n"
                + "          \"role\": \"hazelcast\",\n"
                + "          \"statefulset.kubernetes.io/pod-name\": \"my-release-hazelcast-1\",\n"
                + "          \"app\": \"hazelcast\",\n"
                + "          \"controller-revision-hash\": \"my-release-hazelcast-7bcf66dc79\",\n"
                + "          \"release\": \"my-release\"\n"
                + "        },\n"
                + "        \"annotations\": {\n"
                + "          \"kubernetes.io/limit-ranger\": \"LimitRanger plugin set: cpu request for container my-release-hazelcast\"\n"
                + "        },\n"
                + "        \"ownerReferences\": [\n"
                + "          {\n"
                + "            \"apiVersion\": \"apps/v1beta1\",\n"
                + "            \"kind\": \"StatefulSet\",\n"
                + "            \"name\": \"my-release-hazelcast\",\n"
                + "            \"uid\": \"21b3fb7f-eefd-11e8-ab27-42010a8001ce\",\n"
                + "            \"controller\": true,\n"
                + "            \"blockOwnerDeletion\": true\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"spec\": {\n"
                + "        \"volumes\": [\n"
                + "          {\n"
                + "            \"name\": \"hazelcast-storage\",\n"
                + "            \"configMap\": {\n"
                + "              \"name\": \"my-release-hazelcast-configuration\",\n"
                + "              \"defaultMode\": 420\n"
                + "            }\n"
                + "          },\n"
                + "          {\n"
                + "            \"name\": \"my-release-hazelcast-token-j9db4\",\n"
                + "            \"secret\": {\n"
                + "              \"secretName\": \"my-release-hazelcast-token-j9db4\",\n"
                + "              \"defaultMode\": 420\n"
                + "            }\n"
                + "          }\n"
                + "        ],\n"
                + "        \"containers\": [\n"
                + "          {\n"
                + "            \"name\": \"my-release-hazelcast\",\n"
                + "            \"image\": \"hazelcast/hazelcast:latest\",\n"
                + "            \"ports\": [\n"
                + "              {\n"
                + "                \"name\": \"hazelcast\",\n"
                + "                \"containerPort\": %s,\n"
                + "                \"protocol\": \"TCP\"\n"
                + "              }\n"
                + "            ],\n"
                + "            \"resources\": {\n"
                + "              \"requests\": {\n"
                + "                \"cpu\": \"100m\"\n"
                + "              }\n"
                + "            },\n"
                + "            \"volumeMounts\": [\n"
                + "              {\n"
                + "                \"name\": \"hazelcast-storage\",\n"
                + "                \"mountPath\": \"/data/hazelcast\"\n"
                + "              },\n"
                + "              {\n"
                + "                \"name\": \"my-release-hazelcast-token-j9db4\",\n"
                + "                \"readOnly\": true,\n"
                + "                \"mountPath\": \"/var/run/secrets/kubernetes.io/serviceaccount\"\n"
                + "              }\n"
                + "            ],\n"
                + "            \"terminationMessagePath\": \"/dev/termination-log\",\n"
                + "            \"terminationMessagePolicy\": \"File\",\n"
                + "            \"imagePullPolicy\": \"Always\"\n"
                + "          }\n"
                + "        ],\n"
                + "        \"restartPolicy\": \"Always\",\n"
                + "        \"terminationGracePeriodSeconds\": 30,\n"
                + "        \"dnsPolicy\": \"ClusterFirst\",\n"
                + "        \"serviceAccountName\": \"my-release-hazelcast\",\n"
                + "        \"serviceAccount\": \"my-release-hazelcast\",\n"
                + "        \"nodeName\": \"gke-rafal-test-cluster-default-pool-e5fb2ea5-c7g8\",\n"
                + "        \"securityContext\": {\n"
                + "\n"
                + "        },\n"
                + "        \"hostname\": \"my-release-hazelcast-1\",\n"
                + "        \"schedulerName\": \"default-scheduler\",\n"
                + "        \"tolerations\": [\n"
                + "          {\n"
                + "            \"key\": \"node.kubernetes.io/not-ready\",\n"
                + "            \"operator\": \"Exists\",\n"
                + "            \"effect\": \"NoExecute\",\n"
                + "            \"tolerationSeconds\": 300\n"
                + "          },\n"
                + "          {\n"
                + "            \"key\": \"node.kubernetes.io/unreachable\",\n"
                + "            \"operator\": \"Exists\",\n"
                + "            \"effect\": \"NoExecute\",\n"
                + "            \"tolerationSeconds\": 300\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"status\": {\n"
                + "        \"phase\": \"Running\",\n"
                + "        \"conditions\": [\n"
                + "          {\n"
                + "            \"type\": \"Initialized\",\n"
                + "            \"status\": \"True\",\n"
                + "            \"lastProbeTime\": null,\n"
                + "            \"lastTransitionTime\": \"2018-11-23T08:53:21Z\"\n"
                + "          },\n"
                + "          {\n"
                + "            \"type\": \"Ready\",\n"
                + "            \"status\": \"True\",\n"
                + "            \"lastProbeTime\": null,\n"
                + "            \"lastTransitionTime\": \"2018-11-23T08:53:55Z\"\n"
                + "          },\n"
                + "          {\n"
                + "            \"type\": \"PodScheduled\",\n"
                + "            \"status\": \"True\",\n"
                + "            \"lastProbeTime\": null,\n"
                + "            \"lastTransitionTime\": \"2018-11-23T08:53:21Z\"\n"
                + "          }\n"
                + "        ],\n"
                + "        \"hostIP\": \"10.240.0.18\",\n"
                + "        \"podIP\": \"%s\",\n"
                + "        \"startTime\": \"2018-11-23T08:53:21Z\",\n"
                + "        \"containerStatuses\": [\n"
                + "          {\n"
                + "            \"name\": \"my-release-hazelcast\",\n"
                + "            \"state\": {\n"
                + "              \"running\": {\n"
                + "                \"startedAt\": \"2018-11-23T08:53:23Z\"\n"
                + "              }\n"
                + "            },\n"
                + "            \"lastState\": {\n"
                + "\n"
                + "            },\n"
                + "            \"ready\": true,\n"
                + "            \"restartCount\": 0,\n"
                + "            \"image\": \"hazelcast/hazelcast:latest\",\n"
                + "            \"imageID\": \"docker-pullable://hazelcast/hazelcast@sha256:a4dd478dc792ba3fa560aa41b107fed676b37c283be0306303544a0a8ebcc4c8\",\n"
                + "            \"containerID\": \"docker://705df57d5bfb1417683800aad2b8ac38ba68abcfaf03550d797e36e79313c903\"\n"
                + "          }\n"
                + "        ],\n"
                + "        \"qosClass\": \"Burstable\"\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"metadata\": {\n"
                + "        \"name\": \"my-release-hazelcast-mancenter-f54949c7f-k8vx8\",\n"
                + "        \"generateName\": \"my-release-hazelcast-mancenter-f54949c7f-\",\n"
                + "        \"namespace\": \"default\",\n"
                + "        \"selfLink\": \"/api/v1/namespaces/default/pods/my-release-hazelcast-mancenter-f54949c7f-k8vx8\",\n"
                + "        \"uid\": \"21b7e41c-eefd-11e8-ab27-42010a8001ce\",\n"
                + "        \"resourceVersion\": \"2025\",\n"
                + "        \"creationTimestamp\": \"2018-11-23T08:52:39Z\",\n"
                + "        \"labels\": {\n"
                + "          \"app\": \"hazelcast\",\n"
                + "          \"pod-template-hash\": \"910505739\",\n"
                + "          \"release\": \"my-release\",\n"
                + "          \"role\": \"mancenter\"\n"
                + "        },\n"
                + "        \"annotations\": {\n"
                + "          \"kubernetes.io/limit-ranger\": \"LimitRanger plugin set: cpu request for container my-release-hazelcast-mancenter\"\n"
                + "        },\n"
                + "        \"ownerReferences\": [\n"
                + "          {\n"
                + "            \"apiVersion\": \"extensions/v1beta1\",\n"
                + "            \"kind\": \"ReplicaSet\",\n"
                + "            \"name\": \"my-release-hazelcast-mancenter-f54949c7f\",\n"
                + "            \"uid\": \"21b450e8-eefd-11e8-ab27-42010a8001ce\",\n"
                + "            \"controller\": true,\n"
                + "            \"blockOwnerDeletion\": true\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"spec\": {\n"
                + "        \"volumes\": [\n"
                + "          {\n"
                + "            \"name\": \"mancenter-storage\",\n"
                + "            \"persistentVolumeClaim\": {\n"
                + "              \"claimName\": \"my-release-hazelcast-mancenter\"\n"
                + "            }\n"
                + "          },\n"
                + "          {\n"
                + "            \"name\": \"default-token-cvdgc\",\n"
                + "            \"secret\": {\n"
                + "              \"secretName\": \"default-token-cvdgc\",\n"
                + "              \"defaultMode\": 420\n"
                + "            }\n"
                + "          }\n"
                + "        ],\n"
                + "        \"containers\": [\n"
                + "          {\n"
                + "            \"name\": \"my-release-hazelcast-mancenter\",\n"
                + "            \"image\": \"hazelcast/management-center:latest\",\n"
                + "            \"ports\": [\n"
                + "              {\n"
                + "                \"name\": \"mancenter\",\n"
                + "                \"protocol\": \"TCP\"\n"
                + "              }\n"
                + "            ],\n"
                + "            \"resources\": {\n"
                + "              \"requests\": {\n"
                + "                \"cpu\": \"100m\"\n"
                + "              }\n"
                + "            },\n"
                + "            \"terminationMessagePath\": \"/dev/termination-log\",\n"
                + "            \"terminationMessagePolicy\": \"File\",\n"
                + "            \"imagePullPolicy\": \"Always\"\n"
                + "          }\n"
                + "        ],\n"
                + "        \"restartPolicy\": \"Always\",\n"
                + "        \"terminationGracePeriodSeconds\": 30,\n"
                + "        \"dnsPolicy\": \"ClusterFirst\",\n"
                + "        \"serviceAccountName\": \"default\",\n"
                + "        \"serviceAccount\": \"default\",\n"
                + "        \"nodeName\": \"gke-rafal-test-cluster-default-pool-e5fb2ea5-c7g8\",\n"
                + "        \"securityContext\": {\n"
                + "          \"runAsUser\": 0\n"
                + "        },\n"
                + "        \"schedulerName\": \"default-scheduler\",\n"
                + "        \"tolerations\": [\n"
                + "          {\n"
                + "            \"key\": \"node.kubernetes.io/not-ready\",\n"
                + "            \"operator\": \"Exists\",\n"
                + "            \"effect\": \"NoExecute\",\n"
                + "            \"tolerationSeconds\": 300\n"
                + "          },\n"
                + "          {\n"
                + "            \"key\": \"node.kubernetes.io/unreachable\",\n"
                + "            \"operator\": \"Exists\",\n"
                + "            \"effect\": \"NoExecute\",\n"
                + "            \"tolerationSeconds\": 300\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"status\": {\n"
                + "        \"phase\": \"Running\",\n"
                + "        \"conditions\": [\n"
                + "          {\n"
                + "            \"type\": \"Initialized\",\n"
                + "            \"status\": \"True\",\n"
                + "            \"lastProbeTime\": null,\n"
                + "            \"lastTransitionTime\": \"2018-11-23T08:52:46Z\"\n"
                + "          },\n"
                + "          {\n"
                + "            \"type\": \"Ready\",\n"
                + "            \"status\": \"True\",\n"
                + "            \"lastProbeTime\": null,\n"
                + "            \"lastTransitionTime\": \"2018-11-23T08:53:55Z\"\n"
                + "          },\n"
                + "          {\n"
                + "            \"type\": \"PodScheduled\",\n"
                + "            \"status\": \"True\",\n"
                + "            \"lastProbeTime\": null,\n"
                + "            \"lastTransitionTime\": \"2018-11-23T08:52:46Z\"\n"
                + "          }\n"
                + "        ],\n"
                + "        \"hostIP\": \"10.240.0.18\",\n"
                + "        \"podIP\": \"%s\",\n"
                + "        \"startTime\": \"2018-11-23T08:52:46Z\",\n"
                + "        \"containerStatuses\": [\n"
                + "          {\n"
                + "            \"name\": \"my-release-hazelcast-mancenter\",\n"
                + "            \"state\": {\n"
                + "              \"running\": {\n"
                + "                \"startedAt\": \"2018-11-23T08:53:08Z\"\n"
                + "              }\n"
                + "            },\n"
                + "            \"lastState\": {\n"
                + "\n"
                + "            },\n"
                + "            \"ready\": false,\n"
                + "            \"restartCount\": 0,\n"
                + "            \"image\": \"hazelcast/management-center:latest\",\n"
                + "            \"imageID\": \"docker-pullable://hazelcast/management-center@sha256:0427778a84476a7b11b248b1720d22e43ee689f5148506ea532491aad1a91afa\",\n"
                + "            \"containerID\": \"docker://99ac6e5204876a3106b01d6b3ab2466b1d63c9bd1621d86d7e984840b2c44a3f\"\n"
                + "          }\n"
                + "        ],\n"
                + "        \"qosClass\": \"Burstable\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}", SAMPLE_PORT_1, SAMPLE_ADDRESS_1, SAMPLE_PORT_2, SAMPLE_ADDRESS_2, SAMPLE_NOT_READY_ADDRESS);
    }

    /**
     * Real response recorded from the Kubernetes API call "/api/v1/namespaces/{namespace}/endpoints".
     */
    private static String endpointsListBody() {
        return String.format(
                "{" +
                        "  \"kind\": \"EndpointsList\"," +
                        "  \"apiVersion\": \"v1\"," +
                        "  \"metadata\": {" +
                        "    \"selfLink\": \"/api/v1/namespaces/default/endpoints\"," +
                        "    \"resourceVersion\": \"8792\"" +
                        "  }," +
                        "  \"items\": [" +
                        "    {" +
                        "      \"metadata\": {" +
                        "        \"name\": \"kubernetes\"," +
                        "        \"namespace\": \"default\"," +
                        "        \"selfLink\": \"/api/v1/namespaces/default/endpoints/kubernetes\"," +
                        "        \"uid\": \"01c5aaa4-8411-11e8-abd2-00155d395157\"," +
                        "        \"resourceVersion\": \"38\"," +
                        "        \"creationTimestamp\": \"2018-07-10T07:15:21Z\"" +
                        "      }," +
                        "      \"subsets\": [" +
                        "        {" +
                        "          \"addresses\": [" +
                        "            {" +
                        "              \"ip\": \"%s\"," +
                        "              \"hazelcast-service-port\" :%s" +
                        "            }" +
                        "          ]," +
                        "          \"ports\": [" +
                        "            {" +
                        "              \"name\": \"https\"," +
                        "              \"port\": 8443," +
                        "              \"protocol\": \"TCP\"" +
                        "            }" +
                        "          ]" +
                        "        }" +
                        "      ]" +
                        "    }," +
                        "    {" +
                        "      \"metadata\": {" +
                        "        \"name\": \"my-hazelcast\"," +
                        "        \"namespace\": \"default\"," +
                        "        \"selfLink\": \"/api/v1/namespaces/default/endpoints/my-hazelcast\"," +
                        "        \"uid\": \"80f7f03d-8425-11e8-abd2-00155d395157\"," +
                        "        \"resourceVersion\": \"8788\"," +
                        "        \"creationTimestamp\": \"2018-07-10T09:42:04Z\"," +
                        "        \"labels\": {" +
                        "          \"app\": \"hazelcast\"," +
                        "          \"chart\": \"hazelcast-1.0.0\"," +
                        "          \"heritage\": \"Tiller\"," +
                        "          \"release\": \"my-hazelcast\"" +
                        "        }" +
                        "      }," +
                        "      \"subsets\": [" +
                        "        {" +
                        "          \"addresses\": [" +
                        "            {" +
                        "              \"ip\": \"%s\"," +
                        "              \"nodeName\": \"minikube\"," +
                        "              \"hazelcast-service-port\" : %s," +
                        "              \"targetRef\": {" +
                        "                \"kind\": \"Pod\"," +
                        "                \"namespace\": \"default\"," +
                        "                \"name\": \"my-hazelcast-0\"," +
                        "                \"uid\": \"80f20bcb-8425-11e8-abd2-00155d395157\"," +
                        "                \"resourceVersion\": \"8771\"" +
                        "              }" +
                        "            }" +
                        "          ]," +
                        "          \"notReadyAddresses\": [" +
                        "            {" +
                        "              \"ip\": \"%s\"," +
                        "              \"nodeName\": \"minikube\"," +
                        "              \"targetRef\": {" +
                        "                \"kind\": \"Pod\"," +
                        "                \"namespace\": \"default\"," +
                        "                \"name\": \"my-hazelcast-1\"," +
                        "                \"uid\": \"95bd3636-8425-11e8-abd2-00155d395157\"," +
                        "                \"resourceVersion\": \"8787\"" +
                        "              }" +
                        "            }" +
                        "          ]," +
                        "          \"ports\": [" +
                        "            {" +
                        "              \"name\": \"hzport\"," +
                        "              \"port\": 5701," +
                        "              \"protocol\": \"TCP\"" +
                        "            }" +
                        "          ]" +
                        "        }" +
                        "      ]" +
                        "    }" +
                        "  ]" +
                        "}"
                , SAMPLE_ADDRESS_1, SAMPLE_PORT_1, SAMPLE_ADDRESS_2, SAMPLE_PORT_2, SAMPLE_NOT_READY_ADDRESS);
    }

    /**
     * Real response recorded from the Kubernetes API call "/api/v1/namespaces/{namespace}/endpoints/{endpoint}".
     */
    private static String endpointsBody() {
        return String.format(
                "{" +
                        "  \"kind\": \"Endpoints\"," +
                        "  \"apiVersion\": \"v1\"," +
                        "  \"metadata\": {" +
                        "    \"name\": \"my-hazelcast\"," +
                        "    \"namespace\": \"default\"," +
                        "    \"selfLink\": \"/api/v1/namespaces/default/endpoints/my-hazelcast\"," +
                        "    \"uid\": \"deefb354-8443-11e8-abd2-00155d395157\"," +
                        "    \"resourceVersion\": \"18816\"," +
                        "    \"creationTimestamp\": \"2018-07-10T13:19:27Z\"," +
                        "    \"labels\": {" +
                        "      \"app\": \"hazelcast\"," +
                        "      \"chart\": \"hazelcast-1.0.0\"," +
                        "      \"heritage\": \"Tiller\"," +
                        "      \"release\": \"my-hazelcast\"" +
                        "    }" +
                        "  }," +
                        "  \"subsets\": [" +
                        "    {" +
                        "      \"addresses\": [" +
                        "        {" +
                        "          \"ip\": \"%s\"," +
                        "          \"nodeName\": \"minikube\"," +
                        "          \"hazelcast-service-port\" : %s," +
                        "          \"targetRef\": {" +
                        "            \"kind\": \"Pod\"," +
                        "            \"namespace\": \"default\"," +
                        "            \"name\": \"my-hazelcast-0\"," +
                        "            \"uid\": \"def2f426-8443-11e8-abd2-00155d395157\"," +
                        "            \"resourceVersion\": \"18757\"" +
                        "          }" +
                        "        }," +
                        "        {" +
                        "          \"ip\": \"%s\"," +
                        "          \"nodeName\": \"minikube\"," +
                        "          \"hazelcast-service-port\" : %s," +
                        "          \"targetRef\": {" +
                        "            \"kind\": \"Pod\"," +
                        "            \"namespace\": \"default\"," +
                        "            \"name\": \"my-hazelcast-1\"," +
                        "            \"uid\": \"f3b96106-8443-11e8-abd2-00155d395157\"," +
                        "            \"resourceVersion\": \"18815\"" +
                        "          }" +
                        "        }" +
                        "      ]," +
                        "      \"ports\": [" +
                        "        {" +
                        "          \"name\": \"hzport\"," +
                        "          \"port\": 5701," +
                        "          \"protocol\": \"TCP\"" +
                        "        }" +
                        "      ]" +
                        "    }" +
                        "  ]" +
                        "}"
                , SAMPLE_ADDRESS_1, SAMPLE_PORT_1, SAMPLE_ADDRESS_2, SAMPLE_PORT_2);
    }

    /**
     * Real response recorded from the Kubernetes API call "/api/v1/namespaces/{namespace}/pods/{pod-name}".
     */
    private static String podBody(String nodeName) {
        return String.format(
                "{  \"kind\": \"Pod\",\n"
                        + "  \"apiVersion\": \"v1\",\n"
                        + "  \"metadata\": {\n"
                        + "    \"name\": \"my-release-hazelcast-0\",\n"
                        + "    \"generateName\": \"my-release-hazelcast-\",\n"
                        + "    \"namespace\": \"default\",\n"
                        + "    \"selfLink\": \"/api/v1/namespaces/default/pods/my-release-hazelcast-0\",\n"
                        + "    \"uid\": \"53112ead-0511-11e9-9c53-42010a800013\",\n"
                        + "    \"resourceVersion\": \"7724\",\n"
                        + "    \"creationTimestamp\": \"2018-12-21T11:12:37Z\",\n"
                        + "    \"labels\": {\n"
                        + "      \"app\": \"hazelcast\",\n"
                        + "      \"controller-revision-hash\": \"my-release-hazelcast-695d9d97dd\",\n"
                        + "      \"release\": \"my-release\",\n"
                        + "      \"role\": \"hazelcast\",\n"
                        + "      \"statefulset.kubernetes.io/pod-name\": \"my-release-hazelcast-0\"\n"
                        + "    },\n"
                        + "    \"annotations\": {\n"
                        + "      \"kubernetes.io/limit-ranger\": \"LimitRanger plugin set: cpu request for container my-release-hazelcast\"\n"
                        + "    },\n"
                        + "    \"ownerReferences\": [\n"
                        + "      {\n"
                        + "        \"apiVersion\": \"apps/v1\",\n"
                        + "        \"kind\": \"StatefulSet\",\n"
                        + "        \"name\": \"my-release-hazelcast\",\n"
                        + "        \"uid\": \"53096f3b-0511-11e9-9c53-42010a800013\",\n"
                        + "        \"controller\": true,\n"
                        + "        \"blockOwnerDeletion\": true\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  },\n"
                        + "  \"spec\": {\n"
                        + "    \"volumes\": [\n"
                        + "      {\n"
                        + "        \"name\": \"hazelcast-storage\",\n"
                        + "        \"configMap\": {\n"
                        + "          \"name\": \"my-release-hazelcast-configuration\",\n"
                        + "          \"defaultMode\": 420\n"
                        + "        }\n"
                        + "      },\n"
                        + "      {\n"
                        + "        \"name\": \"my-release-hazelcast-token-j89v4\",\n"
                        + "        \"secret\": {\n"
                        + "          \"secretName\": \"my-release-hazelcast-token-j89v4\",\n"
                        + "          \"defaultMode\": 420\n"
                        + "        }\n"
                        + "      }\n"
                        + "    ],\n"
                        + "    \"containers\": [\n"
                        + "      {\n"
                        + "        \"name\": \"my-release-hazelcast\",\n"
                        + "        \"image\": \"hazelcast/hazelcast:latest\",\n"
                        + "        \"ports\": [\n"
                        + "          {\n"
                        + "            \"name\": \"hazelcast\",\n"
                        + "            \"containerPort\": 5701,\n"
                        + "            \"protocol\": \"TCP\"\n"
                        + "          }\n"
                        + "        ],\n"
                        + "        \"env\": [\n"
                        + "          {\n"
                        + "            \"name\": \"JAVA_OPTS\",\n"
                        + "            \"value\": \"-Dhazelcast.rest.enabled=true -Dhazelcast.config=/data/hazelcast/hazelcast.xml -DserviceName=my-release-hazelcast -Dnamespace=default -Dhazelcast.mancenter.enabled=true -Dhazelcast.mancenter.url=http://my-release-hazelcast-mancenter:8080/hazelcast-mancenter -Dhazelcast.shutdownhook.policy=GRACEFUL -Dhazelcast.shutdownhook.enabled=true -Dhazelcast.graceful.shutdown.max.wait=600 \"\n"
                        + "          }\n"
                        + "        ],\n"
                        + "        \"resources\": {\n"
                        + "          \"requests\": {\n"
                        + "            \"cpu\": \"100m\"\n"
                        + "          }\n"
                        + "        },\n"
                        + "        \"volumeMounts\": [\n"
                        + "          {\n"
                        + "            \"name\": \"hazelcast-storage\",\n"
                        + "            \"mountPath\": \"/data/hazelcast\"\n"
                        + "          },\n"
                        + "          {\n"
                        + "            \"name\": \"my-release-hazelcast-token-j89v4\",\n"
                        + "            \"readOnly\": true,\n"
                        + "            \"mountPath\": \"/var/run/secrets/kubernetes.io/serviceaccount\"\n"
                        + "          }\n"
                        + "        ],\n"
                        + "        \"terminationMessagePath\": \"/dev/termination-log\",\n"
                        + "        \"terminationMessagePolicy\": \"File\",\n"
                        + "        \"imagePullPolicy\": \"Always\"\n"
                        + "      }\n"
                        + "    ],\n"
                        + "    \"restartPolicy\": \"Always\",\n"
                        + "    \"terminationGracePeriodSeconds\": 600,\n"
                        + "    \"dnsPolicy\": \"ClusterFirst\",\n"
                        + "    \"serviceAccountName\": \"my-release-hazelcast\",\n"
                        + "    \"serviceAccount\": \"my-release-hazelcast\",\n"
                        + "    \"nodeName\": \"%s\",\n"
                        + "    \"securityContext\": {\n"
                        + "      \"runAsUser\": 1001,\n"
                        + "      \"fsGroup\": 1001\n"
                        + "    },\n"
                        + "    \"hostname\": \"my-release-hazelcast-0\",\n"
                        + "    \"schedulerName\": \"default-scheduler\",\n"
                        + "    \"tolerations\": [\n"
                        + "      {\n"
                        + "        \"key\": \"node.kubernetes.io/not-ready\",\n"
                        + "        \"operator\": \"Exists\",\n"
                        + "        \"effect\": \"NoExecute\",\n"
                        + "        \"tolerationSeconds\": 300\n"
                        + "      },\n"
                        + "      {\n"
                        + "        \"key\": \"node.kubernetes.io/unreachable\",\n"
                        + "        \"operator\": \"Exists\",\n"
                        + "        \"effect\": \"NoExecute\",\n"
                        + "        \"tolerationSeconds\": 300\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  },\n"
                        + "  \"status\": {\n"
                        + "  }\n"
                        + "}", nodeName);
    }

    /**
     * Real response recorded from the Kubernetes (version < 1.13) API call "/api/v1/nodes/{node-name}".
     */
    private static String nodeBetaBody() {
        return String.format(
                "{"
                        + "  \"kind\": \"Node\",\n"
                        + "  \"apiVersion\": \"v1\",\n"
                        + "  \"metadata\": {\n"
                        + "    \"name\": \"gke-rafal-test-cluster-default-pool-9238654c-12tz\",\n"
                        + "    \"selfLink\": \"/api/v1/nodes/gke-rafal-test-cluster-default-pool-9238654c-12tz\",\n"
                        + "    \"uid\": \"ceab9c17-0508-11e9-9c53-42010a800013\",\n"
                        + "    \"resourceVersion\": \"7954\",\n"
                        + "    \"creationTimestamp\": \"2018-12-21T10:11:39Z\",\n"
                        + "    \"labels\": {\n"
                        + "      \"beta.kubernetes.io/arch\": \"amd64\",\n"
                        + "      \"beta.kubernetes.io/fluentd-ds-ready\": \"true\",\n"
                        + "      \"beta.kubernetes.io/instance-type\": \"n1-standard-1\",\n"
                        + "      \"beta.kubernetes.io/os\": \"linux\",\n"
                        + "      \"cloud.google.com/gke-nodepool\": \"default-pool\",\n"
                        + "      \"cloud.google.com/gke-os-distribution\": \"cos\",\n"
                        + "      \"failure-domain.beta.kubernetes.io/region\": \"us-central1\",\n"
                        + "      \"failure-domain.beta.kubernetes.io/zone\": \"%s\",\n"
                        + "      \"kubernetes.io/hostname\": \"gke-rafal-test-cluster-default-pool-9238654c-12tz\"\n"
                        + "    },\n"
                        + "    \"annotations\": {\n"
                        + "      \"node.alpha.kubernetes.io/ttl\": \"0\",\n"
                        + "      \"volumes.kubernetes.io/controller-managed-attach-detach\": \"true\"\n"
                        + "    }\n"
                        + "  },\n"
                        + "  \"spec\": {\n"
                        + "  },\n"
                        + "  \"status\": {\n"
                        + "  }\n"
                        + "}", ZONE);
    }

    /**
     * Real response recorded from the Kubernetes (version >= 1.13) API call "/api/v1/nodes/{node-name}".
     */
    private static String nodeBody() {
        return String.format(
                "{"
                        + "  \"kind\": \"Node\",\n"
                        + "  \"apiVersion\": \"v1\",\n"
                        + "  \"metadata\": {\n"
                        + "    \"name\": \"gke-rafal-test-cluster-default-pool-9238654c-12tz\",\n"
                        + "    \"selfLink\": \"/api/v1/nodes/gke-rafal-test-cluster-default-pool-9238654c-12tz\",\n"
                        + "    \"uid\": \"ceab9c17-0508-11e9-9c53-42010a800013\",\n"
                        + "    \"resourceVersion\": \"7954\",\n"
                        + "    \"creationTimestamp\": \"2018-12-21T10:11:39Z\",\n"
                        + "    \"labels\": {\n"
                        + "      \"beta.kubernetes.io/arch\": \"amd64\",\n"
                        + "      \"beta.kubernetes.io/fluentd-ds-ready\": \"true\",\n"
                        + "      \"beta.kubernetes.io/instance-type\": \"n1-standard-1\",\n"
                        + "      \"beta.kubernetes.io/os\": \"linux\",\n"
                        + "      \"cloud.google.com/gke-nodepool\": \"default-pool\",\n"
                        + "      \"cloud.google.com/gke-os-distribution\": \"cos\",\n"
                        + "      \"failure-domain.beta.kubernetes.io/region\": \"deprecated-region\",\n"
                        + "      \"failure-domain.beta.kubernetes.io/zone\": \"deprecated-zone\",\n"
                        + "      \"failure-domain.kubernetes.io/region\": \"us-central1\",\n"
                        + "      \"failure-domain.kubernetes.io/zone\": \"%s\",\n"
                        + "      \"kubernetes.io/hostname\": \"gke-rafal-test-cluster-default-pool-9238654c-12tz\"\n"
                        + "    },\n"
                        + "    \"annotations\": {\n"
                        + "      \"node.alpha.kubernetes.io/ttl\": \"0\",\n"
                        + "      \"volumes.kubernetes.io/controller-managed-attach-detach\": \"true\"\n"
                        + "    }\n"
                        + "  },\n"
                        + "  \"spec\": {\n"
                        + "  },\n"
                        + "  \"status\": {\n"
                        + "  }\n"
                        + "}", ZONE);
    }

    /**
     * Real response recorded from the Kubernetes API.
     */
    private static String forbiddenBody() {
        return "Forbidden!Configured service account doesn't have access. Service account may have been revoked. "
                + "endpoints is forbidden: User \"system:serviceaccount:default:default\" cannot list endpoints "
                + "in the namespace \"default\"";
    }

    private static String malformedBody() {
        return "malformed response";
    }

    private static List<String> extractIpPort(List<EntrypointAddress> addresses) {
        List<String> result = new ArrayList<String>();
        for (EntrypointAddress address : addresses) {
            String ip = address.getIp();
            Integer port = address.getPort();
            result.add(ipPort(ip, port));
        }
        return result;
    }

    private static String ipPort(String ip, Integer port) {
        return String.format("%s:%s", ip, port);
    }
}
