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

package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.TestProcessors;
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

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SimpleTestInClusterSupportTest extends SimpleTestInClusterSupport {

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
    public void jobShouldNotBeVisibleAfterFirstRestart() {
        // given
        Job job = hz().getJet().newJob(TestProcessors.streamingDag());

        // when
        supportAfter();

        // then
        assertThat(hz().getJet().getJobs()).isEmpty();
        assertThatThrownBy(() -> job.getStatus()).isInstanceOf(JobNotFoundException.class);
    }

    @Test
    public void jobShouldNotBeVisibleAfterSecondRestart() {
        // given
        Job job = hz().getJet().newJob(TestProcessors.batchDag());
        supportAfter();
        Job job2 = hz().getJet().newJob(TestProcessors.batchDag());

        // when
        supportAfter();

        // then
        assertThat(hz().getJet().getJobs()).isEmpty();
        assertThatThrownBy(() -> job.getStatus()).isInstanceOf(JobNotFoundException.class);
        assertThatThrownBy(() -> job2.getStatus()).isInstanceOf(JobNotFoundException.class);
    }
}
