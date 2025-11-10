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

package com.hazelcast.kubernetes;

import com.github.tomakehurst.wiremock.client.VerificationException;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.hazelcast.instance.impl.ClusterTopologyIntent;
import com.hazelcast.instance.impl.ClusterTopologyIntentTracker;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.kubernetes.KubernetesConfig.ExposeExternallyMode;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.moreThan;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

@QuickTest
@WireMockTest
class StsDetermineRootNameTest {

    private static final String GET_STS_PATH = "/apis/apps/v1/namespaces/default/statefulsets";
    private static final String RESOURCE_VERSION = "123";

    @Test
    void podIsNotLite(WireMockRuntimeInfo wmRuntimeInfo) {
        JsonObject input = Json.parse("""
                {
                    "items": [
                        {"metadata": {"name": "hazelcast-lite"},"spec":{},"status":{}},
                        {"metadata": {"name": "hazelcast-lite-lite"},"spec":{},"status":{}},
                        {"metadata": {"name": "hazelcast"},"spec":{},"status":{}},
                        {"metadata": {"name": "other"},"spec":{},"status":{}}
                    ],
                    "metadata": {
                        "resourceVersion": "%s"
                    }
                }
                """.formatted(RESOURCE_VERSION)).asObject();

        testImpl(wmRuntimeInfo, "hazelcast-123", input, Set.of("hazelcast", "hazelcast-lite"), 1);
    }

    @Test
    void podIsLiteAndRootNotSuffixedWithLite(WireMockRuntimeInfo wmRuntimeInfo) {
        JsonObject input = Json.parse("""
                {
                    "items": [
                        {"metadata":{"name":"hazelcast-lite"},"spec":{},"status":{}},
                        {"metadata":{"name":"hazelcast"},"spec":{},"status":{}},
                        {"metadata":{"name":"other"},"spec":{},"status":{}}
                    ],
                    "metadata": {
                        "resourceVersion": "%s"
                    }
                }
                """.formatted(RESOURCE_VERSION)).asObject();

        testImpl(wmRuntimeInfo, "hazelcast-lite-2", input, Set.of("hazelcast", "hazelcast-lite"), 2);
    }

    @Test
    void podIsLiteAndRootSuffixedWithLite(WireMockRuntimeInfo wmRuntimeInfo) {
        JsonObject input = Json.parse("""
                {
                    "items": [
                        {"metadata":{"name":"hazelcast-lite"},"spec":{},"status":{}},
                        {"metadata":{"name":"hazelcast-lite-lite"},"spec":{},"status":{}},
                        {"metadata":{"name":"hazelcast"},"spec":{},"status":{}},
                        {"metadata":{"name":"other"},"spec":{},"status":{}}
                    ],
                    "metadata": {
                        "resourceVersion": "%s"
                    }
                }
                """.formatted(RESOURCE_VERSION)).asObject();

        testImpl(wmRuntimeInfo, "hazelcast-lite-lite-12", input, Set.of("hazelcast-lite", "hazelcast-lite-lite"), 2);
    }

    @Test
    void testCloudNameFormat(WireMockRuntimeInfo wmRuntimeInfo) {
        JsonObject input = Json.parse("""
                {
                    "items": [
                        {"metadata":{"name":"sts-87zcc84t-akezboms-46"},"spec":{},"status":{}},
                        {"metadata":{"name":"sts-87zcc84t-lhxuqckz-46"},"spec":{},"status":{}},
                        {"metadata":{"name":"sts-87zcc84t-n00i3o4v-46"},"spec":{},"status":{}}
                    ],
                    "metadata": {
                        "resourceVersion": "%s"
                    }
                }
                """.formatted(RESOURCE_VERSION)).asObject();

        testImpl(wmRuntimeInfo, "sts-87zcc84t-lhxuqckz-46-1", input, Set.of("sts-87zcc84t-lhxuqckz-46"), 1);
    }

    @Test
    void failureOnAmbiguousState(WireMockRuntimeInfo wmRuntimeInfo) {
        JsonObject input = Json.parse("""
                {
                    "items": [
                        {"metadata":{"name":"hazelcast-lite"},"spec":{},"status":{}},
                        {"metadata":{"name":"hazelcast-lite-lite"},"spec":{},"status":{}},
                        {"metadata":{"name":"hazelcast"},"spec":{},"status":{}},
                        {"metadata":{"name":"other"},"spec":{},"status":{}}
                    ],
                    "metadata": {
                        "resourceVersion": "%s"
                    }
                }
                """.formatted(RESOURCE_VERSION)).asObject();

        assertThatThrownBy(() -> testImpl(wmRuntimeInfo, "hazelcast-lite", input, Set.of(), 1)).isInstanceOf(
                IllegalStateException.class).hasMessageContaining("[hazelcast-lite, hazelcast-lite-lite, hazelcast, other]");
    }

    void testImpl(WireMockRuntimeInfo wmRuntimeInfo, String podName, JsonObject getStsResponse, Set<String> expectedStsToWatch,
                  int getStsRequestLimit) {
        TestTopologyTracker tracker = new TestTopologyTracker();
        stubGetStsWithRequestLimit(getStsResponse, getStsRequestLimit);
        KubernetesClient client = createClient(podName, wmRuntimeInfo.getHttpBaseUrl(), tracker);
        String watchUrl = "/apis/apps/v1/namespaces/default/statefulsets?watch=1&resourceVersion=" + RESOURCE_VERSION;
        stubFor(get(watchUrl).willReturn(aResponse().withStatus(200).withBody(buildWatchResponseFrom(getStsResponse))));
        try {
            client.start();
            waitForStsRequestsToExceedLimit(getStsRequestLimit);
            // At this point we know we processed every line in the stubbed watch response so can terminate the watch thread
            client.destroy();
            List<String> allStsNames = getStsNames(getStsResponse);
            int expectedReadyReplicas = expectedStsToWatch.stream().mapToInt(allStsNames::indexOf).filter(n -> n >= 0)
                                                          .map(this::tenToThe).sum();
            assertThat(tracker.readyReplicas).isEqualTo(expectedReadyReplicas);
        } finally {
            client.destroy();
        }
    }

    private void waitForStsRequestsToExceedLimit(int limit) {
        int retries = 20;
        for (int i = 0; i < retries; i++) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                fail("Test was interrupted");
            }
            try {
                verify(moreThan(limit), getRequestedFor(urlEqualTo(GET_STS_PATH)));
                break;
            } catch (VerificationException e) {
                if (i == retries - 1) {
                    throw e;
                }
            }
        }
    }

    private void stubGetStsWithRequestLimit(JsonObject response, int limit) {
        for (int i = 0; i < limit; i++) {
            String state = i == 0 ? STARTED : "Called " + i;
            stubFor(get(GET_STS_PATH).inScenario("Limited").whenScenarioStateIs(state)
                                     .willReturn(aResponse().withStatus(200).withBody(response.toString()))
                                     .willSetStateTo("Called " + (i + 1)));
        }
        stubFor(get(GET_STS_PATH).inScenario("Limited").whenScenarioStateIs("Called " + limit)
                                 .willReturn(aResponse().withStatus(500)));
    }

    private String buildWatchResponseFrom(JsonObject getStsResponse) {
        StringBuilder sb = new StringBuilder();
        JsonArray items = getStsResponse.get("items").asArray();
        for (int i = 0; i < items.size(); i++) {
            String name = items.get(i).asObject().get("metadata").asObject().get("name").asString();
            sb.append("""
                    {"type":"MODIFIED","object":{"metadata":{"name":"%s"},"spec":{},"status":{"readyReplicas":%s}}}
                    """.formatted(name, tenToThe(i)));
        }
        return sb.toString();
    }

    private int tenToThe(int power) {
        return (int) Math.round(Math.pow(10, power));
    }

    private List<String> getStsNames(JsonObject getStsResponse) {
        return StreamSupport.stream(getStsResponse.get("items").asArray().spliterator(), false)
                            .map(item -> item.asObject().get("metadata").asObject().get("name").asString()).toList();
    }

    KubernetesClient createClient(String podName, String masterUrl, ClusterTopologyIntentTracker tracker) {
        return new KubernetesClient("default", podName, masterUrl, new TestTokenProvider(), "", 0, ExposeExternallyMode.DISABLED,
                false, "label", "value", tracker, new KubernetesApiEndpointProvider());
    }

    private static class TestTokenProvider
            implements KubernetesTokenProvider {

        @Override
        public String getToken() {
            return "token";
        }
    }

    private static class TestTopologyTracker
            implements ClusterTopologyIntentTracker {

        int readyReplicas = 0;

        @Override
        public void update(int previousSpecifiedReplicas, int updatedSpecifiedReplicas, int previousReadyReplicas,
                           int updatedReadyReplicas, int previousCurrentReplicas, int updatedCurrentReplicas) {
            readyReplicas = updatedReadyReplicas;
        }

        @Override
        public ClusterTopologyIntent getClusterTopologyIntent() {
            return null;
        }

        @Override
        public void initialize() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public void initializeClusterTopologyIntent(ClusterTopologyIntent clusterTopologyIntent) {
        }

        @Override
        public void shutdownWithIntent(ClusterTopologyIntent clusterTopologyIntent) {
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public int getCurrentSpecifiedReplicaCount() {
            return 0;
        }

        @Override
        public void onMembershipChange() {
        }
    }
}
