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
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.kubernetes.assertion.PodAssertions.assertThatPod;
import static org.awaitility.Awaitility.await;

class HazelcastHelmTestHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastHelmTestHelper.class);

    private static final int MC_MEMBER_COUNT = 1;
    private final KubernetesTestHelper kubernetesTestHelper;
    private final Helm helm;

    public HazelcastHelmTestHelper(KubernetesTestHelper kubernetesTestHelper) {
        this.kubernetesTestHelper = kubernetesTestHelper;
        this.helm = kubernetesTestHelper.helm();
    }

    public void installHazelcastUsingHelmChart(String releaseName, int hzMemberCount) throws MalformedURLException {
        helm.repo().add("hazelcast", new URL("https://hazelcast-charts.s3.amazonaws.com/"));
        helm.repo().update();
        helm.upgradeCommand(releaseName, "hazelcast/hazelcast")
                .withInstall()
                .addValue("cluster.memberCount", hzMemberCount)
                .run();
        assertEventuallyPodsRunning(hzMemberCount + MC_MEMBER_COUNT, releaseName);
    }

    void installCustomHazelcastUsingHelmChart(String releaseName, String imageRepository, String imageTag, int hzMemberCount) throws MalformedURLException {
        helm.repo().add("hazelcast", new URL("https://hazelcast-charts.s3.amazonaws.com/"));
        helm.repo().update();
        helm.upgradeCommand(releaseName, "hazelcast/hazelcast")
                .withInstall()
                .addValue("cluster.memberCount", hzMemberCount)
                .addValue("image.repository", imageRepository)
                .addValue("image.tag", imageTag)
                .addValue("image.pullPolicy", "Always")
                .run();
        assertEventuallyPodsRunning(hzMemberCount + MC_MEMBER_COUNT, releaseName);
    }

    private void assertEventuallyPodsRunning(int expectedCount, String prefix) {
        LOGGER.info("Waiting for {} running pods prefixed with {}", expectedCount, prefix);
        await().atMost(10, TimeUnit.MINUTES)
                .untilAsserted(() -> Assertions.assertThat(kubernetesTestHelper.allPodsStartingWith(prefix))
                        .allSatisfy(pod -> assertThatPod(pod).isRunning())
                        .hasSize(expectedCount)
                );
    }
}
