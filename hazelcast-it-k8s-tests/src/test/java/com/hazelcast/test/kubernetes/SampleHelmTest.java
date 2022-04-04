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
package com.hazelcast.test.kubernetes;

import com.hazelcast.test.kubernetes.helm.Helm;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.kubernetes.assertion.PodAssertions.assertThatPod;
import static io.restassured.RestAssured.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class SampleHelmTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleHelmTest.class);
    private static final String NAMESPACE = SampleHelmTest.class.getSimpleName().toLowerCase(Locale.ROOT);
    private static final String RELEASE_NAME = "hz-test-release";
    private static final int HZ_MEMBER_COUNT = 3;
    private static final int MC_MEMBER_COUNT = 1;

    private static final KubernetesTestHelper testHelper = new KubernetesTestHelper(NAMESPACE);
    private static final Helm helm = testHelper.helm();
    private static final HazelcastHelmTestHelper hazelcastHelmTestHelper = new HazelcastHelmTestHelper(testHelper);

    @BeforeClass
    public static void beforeClass() throws IOException {
        testHelper.createOrReplaceNamespace(NAMESPACE);
        hazelcastHelmTestHelper.installHazelcastUsingHelmChart(RELEASE_NAME, HZ_MEMBER_COUNT);
    }

    @Test
    public void hz_instances_should_be_scalable() {
        listPods();

        scaleHzMembers(HZ_MEMBER_COUNT - 1);
        assertEventuallyPodsRunning(HZ_MEMBER_COUNT - 1 + MC_MEMBER_COUNT, RELEASE_NAME);
        listPods();

        scaleHzMembers(HZ_MEMBER_COUNT);
        assertEventuallyPodsRunning(HZ_MEMBER_COUNT + MC_MEMBER_COUNT, RELEASE_NAME);
        listPods();
    }

    @Test
    public void mancenter_should_accessible_from_outside() {
        String externalIp = testHelper.getServiceExternalIp(RELEASE_NAME + "-hazelcast-mancenter");
        String mancenterUrl = "http://" + externalIp + ":8080";

        LOGGER.info("Management center: {}", mancenterUrl);

        when().get(mancenterUrl)
                .then().statusCode(200);
    }

    @Test
    public void hz_instance_should_started_and_log() {
        String log = testHelper.kubernetesClient().pods().withName(RELEASE_NAME + "-hazelcast-0").getLog();
        assertThat(log).contains(":5701 is STARTED");
    }

    @AfterClass
    public static void afterClass() {
        //comment out to make re-runs faster (although the namespace state can be polluted by previous runs)
        removeNamespace();
        testHelper.close();
    }

    private static void removeNamespace() {
        helm.uninstallCommand(RELEASE_NAME).run();
        removeMancenterPVC();
        testHelper.deleteNamespace(NAMESPACE);
    }

    private static void removeMancenterPVC() {
        testHelper.kubernetesClient().persistentVolumeClaims().withLabel("role", "mancenter").delete();
    }

    private static void assertEventuallyPodsRunning(int expectedCount, String prefix) {
        LOGGER.info("Waiting for {} running pods prefixed with {}", expectedCount, prefix);
        await().atMost(10, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(testHelper.allPodsStartingWith(prefix))
                        .allSatisfy(pod -> assertThatPod(pod).isRunning())
                        .hasSize(expectedCount)
                );
    }

    private static void scaleHzMembers(int memberCount) {
        LOGGER.info("Scaling HZ member to {} ", memberCount);

        helm.upgradeCommand(RELEASE_NAME, "hazelcast/hazelcast")
                .withInstall()
                .addValue("cluster.memberCount", memberCount).run();
    }

    private static void listPods() {
        LOGGER.info("Pods:");
        testHelper.allPodsStartingWith(RELEASE_NAME).stream()
                .map(p -> "Pod: " + p.getMetadata().getName() + " namespace: " + p.getMetadata().getNamespace() + " status: " + p.getStatus().getPhase())
                .forEach(LOGGER::info);
    }


}
