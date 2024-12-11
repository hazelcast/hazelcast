/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.impl.ClusterTopologyIntentTracker;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.kubernetes.KubernetesClient.StsMonitorThread;
import com.hazelcast.spi.utils.RestClient;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import io.fabric8.kubernetes.api.model.ListMetaBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.WatchEvent;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import io.fabric8.kubernetes.api.model.apps.StatefulSetListBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetStatusBuilder;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.fabric8.mockwebserver.dsl.ReturnOrWebsocketable;
import io.fabric8.mockwebserver.dsl.TimesOnceableOrHttpHeaderable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// test interaction of KubernetesClient with Kubernetes mock API server
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class StsMonitorTest {

    private static final String SERVICE_NAME = "hz-hazelcast";

    @Rule
    public KubernetesServer kubernetesServer = new KubernetesServer(false);

    String namespace;
    String apiServerBaseUrl;
    String token;
    ClusterTopologyIntentTracker tracker;
    KubernetesClient client;
    StsMonitorThread stsMonitor;

    @Before
    public void setup() {
        tracker = mock(ClusterTopologyIntentTracker.class);
        when(tracker.isEnabled()).thenReturn(true);

        NamespacedKubernetesClient mockServerClient = kubernetesServer.getClient();
        namespace = mockServerClient.getNamespace();
        apiServerBaseUrl = mockServerClient.getMasterUrl().toString();
        if (apiServerBaseUrl.endsWith("/")) {
            // our KubernetesClient expects a base url without trailing /
            apiServerBaseUrl = apiServerBaseUrl.substring(0, apiServerBaseUrl.length() - 1);
        }
        token = mockServerClient.getConfiguration().getOauthToken();
    }

    private void buildKubernetesClient() {
        StaticTokenProvider tokenProvider = new StaticTokenProvider(token);
        client = new KubernetesClient(namespace, SERVICE_NAME, apiServerBaseUrl, tokenProvider, null,
                3, KubernetesConfig.ExposeExternallyMode.DISABLED, false,
                null, null, tracker, null);
        stsMonitor = client.stsMonitorThread;
    }

    @Test
    public void testInitialStsList() {
        expectAndReturnStsList("1", "2").always();

        buildKubernetesClient();
        stsMonitor.readInitialStsList();
        RuntimeContext runtimeContext = stsMonitor.latestRuntimeContext;
        assertEquals("1", runtimeContext.getResourceVersion());
        assertEquals(3, runtimeContext.getCurrentReplicas());
        assertEquals(3, runtimeContext.getReadyReplicas());
        assertEquals(3, runtimeContext.getSpecifiedReplicaCount());
    }

    @Test
    public void testWatchSts() throws IOException {
        expectAndReturnStsList("1", "2").once();
        kubernetesServer.expect().get()
                .withPath("/apis/apps/v1/namespaces/" + namespace + "/statefulsets?watch=1&resourceVersion=1")
                .andReturn(200, new WatchEvent(buildDefaultSts("4"), "MODIFIED"))
                .always();

        buildKubernetesClient();
        stsMonitor.readInitialStsList();
        RestClient.WatchResponse watchResponse = stsMonitor.sendWatchRequest();
        String message = watchResponse.nextLine();
        stsMonitor.onWatchEventReceived(message);
        RuntimeContext runtimeContext = stsMonitor.latestRuntimeContext;
        assertEquals("4", runtimeContext.getResourceVersion());
        assertEquals(3, runtimeContext.getCurrentReplicas());
        assertEquals(3, runtimeContext.getReadyReplicas());
        assertEquals(3, runtimeContext.getSpecifiedReplicaCount());
    }

    @Test
    public void testWatchResumesAfter410Gone() {
        buildKubernetesClient();

        // initial STS list
        expectAndReturnStsList("1", "2").once();
        // first watch request fails with 410 GONE
        expectWatch("1").andReturn(410, null).once();
        // second STS list (as StsMonitor re-initializes)
        expectAndReturnStsList("3", "4").once();
        // second watch request accepted and sends an event
        expectWatch("3")
                .andReturn(200, new WatchEvent(buildDefaultSts("5"), "MODIFIED"))
                .once();
        // next event replies with HTTP code 500
        expectWatch("5").andReturn(500, null).once();
        // attempts to initialize sts list again
        expectStsList().andReply(200, request -> {
            // after failure with HTTP code 500, stsMonitor retries reading the sts list
            // let's stop the stsMonitor run loop here
            stsMonitor.running = false;
            return buildDefaultStsList("1", "2");
        }).once();

        // stsMonitor.run():
        // - gets initial list of statefulsets
        // - issues watch request
        // - if response is 410 GONE, lists STS's again and resumes watch
        stsMonitor.run();

        // verify
        // 1st time initialization: StsMonitor reads initial statefulset list and provides update
        verify(tracker, times(1)).update(-1, 3, -1, 3, -1, 3);
        // during resume, tracker is updated
        verify(tracker, times(3)).update(3, 3, 3, 3, 3, 3);
    }

    @Test
    public void testStsMonitor_whenKubernetesApiWatchFailure() {
        // sts list succeeds, but watch always fails
        expectAndReturnStsList("1", "2").always();
        expectWatch("1").andReturn(500, null).always();

        buildKubernetesClient();
        Future<?> runFuture = spawn(stsMonitor::run);
        sleepSeconds(10);
        stsMonitor.running = false;
        FutureUtil.waitWithDeadline(Collections.singleton(runFuture), 5, TimeUnit.SECONDS);
        assertTrue("Backoff should be triggered due to API faults and idleCount should be > 0",
                stsMonitor.idleCount > 0);
    }

    @Test
    public void testStsMonitor_whenKubernetesApiListFailure() {
        // sts list fails
        expectStsList().andReturn(500, null).always();

        buildKubernetesClient();
        Future<?> runFuture = spawn(stsMonitor::run);
        sleepSeconds(10);
        stsMonitor.running = false;
        FutureUtil.waitWithDeadline(Collections.singleton(runFuture), 5, TimeUnit.SECONDS);
        assertTrue("Backoff should be triggered due to API faults and idleCount should be > 0",
                stsMonitor.idleCount > 0);
    }

    @Test
    public void testStsMonitorThread_terminatedPromptlyOnClientShutdown() {
        expectAndReturnStsList("1", "2").always();
        kubernetesServer.expect().get()
                .withPath("/apis/apps/v1/namespaces/" + namespace + "/statefulsets?watch=1&resourceVersion=1")
                .andReturn(200, new WatchEvent(buildDefaultSts("4"), "MODIFIED"))
                .always();
        kubernetesServer.expect().get()
                .withPath("/apis/discovery.k8s.io/v1/namespaces/" + namespace + "/endpointslices")
                .andReturn(200, "{}")
                .once();

        // Create new client instance which uses the topology intent tracker
        buildKubernetesClient();
        client.start();

        // Wait until we've received some watch data (to ensure our thread is reading connections)
        assertTrueEventually(() -> assertNotNull(stsMonitor.watchResponse));

        // Trigger shutdown of the sts monitor thread and assert it has finished after call completion
        client.destroy();
        assertTrue(stsMonitor.finished);
    }

    // respond with 200 OK and statefulset list
    private TimesOnceableOrHttpHeaderable expectAndReturnStsList(String listResourceVersion, String stsResourceVersion) {
        return expectStsList().andReturn(200, buildDefaultStsList(listResourceVersion, stsResourceVersion));
    }

    private ReturnOrWebsocketable<TimesOnceableOrHttpHeaderable<Void>> expectWatch(String resourceVersion) {
        return expectPath(watchUrl(resourceVersion));
    }

    private ReturnOrWebsocketable<TimesOnceableOrHttpHeaderable<Void>> expectStsList() {
        return expectPath(stsListUrl());
    }

    private ReturnOrWebsocketable<TimesOnceableOrHttpHeaderable<Void>> expectPath(String path) {
        return kubernetesServer.expect().get().withPath(path);
    }

    private String watchUrl(String resourceVersion) {
        return "/apis/apps/v1/namespaces/" + namespace + "/statefulsets"
                + "?watch=1&resourceVersion=" + resourceVersion;
    }

    private String stsListUrl() {
        return "/apis/apps/v1/namespaces/" + namespace + "/statefulsets";
    }

    StatefulSetList buildDefaultStsList(String resourceVersion, String stsResourceVersion) {
        return new StatefulSetListBuilder().withItems(buildDefaultSts(stsResourceVersion))
                .withMetadata(new ListMetaBuilder().withResourceVersion(resourceVersion).build())
                .build();
    }

    StatefulSet buildDefaultSts(String resourceVersion) {
        return buildSts("default", SERVICE_NAME, 3, 3, 3, 3, resourceVersion);
    }

    StatefulSet buildSts(String namespace, String name, int specReplicas, int replicas,
                         int currentReplicas, int readyReplicas, String resourceVersion) {
        return new StatefulSetBuilder()
                .withSpec(new StatefulSetSpecBuilder().withReplicas(specReplicas).build())
                .withStatus(new StatefulSetStatusBuilder()
                        .withReplicas(replicas)
                        .withCurrentReplicas(currentReplicas)
                        .withReadyReplicas(readyReplicas)
                        .build())
                .withMetadata(new ObjectMetaBuilder()
                        .withNamespace(namespace)
                        .withName(name)
                        .withResourceVersion(resourceVersion)
                        .build())
                .build();
    }
}
