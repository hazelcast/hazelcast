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

package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SimpleTestInClusterSupportTest extends SimpleTestInClusterSupport {

    private static final String MAP_NAME = "somemap";

    @Parameterized.Parameter
    public boolean useClient;

    @Parameterized.Parameters(name = "useClient={0}")
    public static Collection<Object> data() {
        return asList(false, true);
    }

    @Before
    public void before() {
        if (useClient) {
            initializeWithClient(1, null, null);
        } else {
            initialize(1, null);
        }
    }

    @After
    public void after() throws Exception {
        // these tests check cleanup in SimpleTestInClusterSupport
        // to check that in isolated way we use initialize/initializeWithClient per test method
        // unlike standard invocation per class.
        supportAfter();
        supportAfterClass();
    }

    private HazelcastInstance hz() {
        return useClient ? client() : instance();
    }

    @Test
    public void imapShouldNotBeVisibleAfterFirstRestart() {
        // given
        createImap();

        // when
        supportAfter();

        // then
        assertImapDoesNotExist();
    }

    @Test
    public void imapShouldNotBeVisibleAfterSecondRestart() {
        // given
        createImap();
        supportAfter();
        createImap();

        // when
        supportAfter();

        // then
        assertImapDoesNotExist();
    }

    @Test
    public void testPreservedImapIsNotClearedAfterSecondRestart() {
        // This is not a desired scenario, but this test demonstrates limitation to clean up via DistributedObjects.
        // If this behavior is ever changed, clean up in supportAfter method may be simplified.
        IMap<String, String> im = createImap();

        assertThat(hz().getDistributedObjects()).hasSize(1);

        supportAfter();

        assertImapDoesNotExist();

        im.put("c", "d");
        assertThat(im.values()).containsExactlyInAnyOrder("d");
        assertThat((Map<String, String>) im).hasSize(1);
        assertImapDoesNotExist();

        supportAfter();

        assertThat(im.values()).containsExactlyInAnyOrder("d");
        assertThat((Map<String, String>) im).hasSize(1);
        assertImapDoesNotExist();
    }

    private IMap<String, String> createImap() {
        IMap<String, String> map = hz().getMap(MAP_NAME);
        map.put("a", "b");
        return map;
    }

    private void assertImapDoesNotExist() {
        assertThat(hz().getDistributedObjects()).isEmpty();
    }

    @Test
    public void jobShouldNotBeVisibleAfterFirstRestart() {
        // given
        Job job = hz().getJet().newJob(TestProcessors.streamingDag());

        // when
        supportAfter();

        // then
        assertThat(hz().getJet().getJobs()).isEmpty();
        assertThatThrownBy(job::getStatus).isInstanceOf(JobNotFoundException.class);
    }

    @Test
    public void jobShouldNotBeVisibleAfterSecondRestart() {
        // given
        Job job = hz().getJet().newJob(TestProcessors.streamingDag());
        supportAfter();
        Job job2 = hz().getJet().newJob(TestProcessors.streamingDag());

        // when
        supportAfter();

        // then
        assertThat(hz().getJet().getJobs()).isEmpty();
        assertThatThrownBy(job::getStatus).isInstanceOf(JobNotFoundException.class);
        assertThatThrownBy(job2::getStatus).isInstanceOf(JobNotFoundException.class);
    }
}
