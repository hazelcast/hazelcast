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

import static io.restassured.RestAssured.when;

public class SampleCustomCodebaseHelmTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleCustomCodebaseHelmTest.class);
    private static final String NAMESPACE = SampleCustomCodebaseHelmTest.class.getSimpleName().toLowerCase(Locale.ROOT);
    private static final String RELEASE_NAME = "hz-test-release";
    private static final int HZ_MEMBER_COUNT = 2;

    private static final KubernetesTestHelper testHelper = new KubernetesTestHelper(NAMESPACE);
    private static final Helm helm = testHelper.helm();
    private static final HazelcastHelmTestHelper hazelcastHelmTestHelper = new HazelcastHelmTestHelper(testHelper);

    @BeforeClass
    public static void beforeClass() throws IOException {
        testHelper.createOrReplaceNamespace(NAMESPACE);
        String imageRepository = "us-east1-docker.pkg.dev/hazelcast-33/hazelcast-it-k8s-tests/hazelcast";
        String imageTag = System.getProperty("hazelcast.it.k8s.docker.tag", "test-k8s");
        hazelcastHelmTestHelper.installCustomHazelcastUsingHelmChart(RELEASE_NAME, imageRepository, imageTag, HZ_MEMBER_COUNT);
    }

    @Test
    public void mancenter_should_accessible_from_outside() {
        String externalIp = testHelper.getServiceExternalIp(RELEASE_NAME + "-hazelcast-mancenter");
        String mancenterUrl = "http://" + externalIp + ":8080";

        LOGGER.info("Management center: {}", mancenterUrl);

        when().get(mancenterUrl)
                .then().statusCode(200);
    }

    @AfterClass
    public static void afterClass() {
        //comment out to make re-runs faster (although the namespace state can be polluted by previous runs)
        removeNamespace();
        testHelper.close();
    }

    private static void removeNamespace() {
        helm.uninstallCommand(RELEASE_NAME).run();
        testHelper.deleteNamespace(NAMESPACE);
    }

}
