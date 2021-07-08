/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.TestProcessors.batchDag;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
public class LightJob_StandaloneClusterTest extends JetTestSupport {

    @Test
    public void test_submittedFromLiteMember() {
        HazelcastInstance nonLiteInst = createHazelcastInstance();
        HazelcastInstance liteInst = createHazelcastInstance(smallInstanceConfig().setLiteMember(true));
        // lite members can be coordinators, though they won't execute processors
        Job job = liteInst.getJet().newLightJob(batchDag());

        assertTrueEventually(() -> assertLightJobExecuting(job, nonLiteInst));
        assertLightJobNotExecuting(job, liteInst);
    }

    @Test
    public void test_submittedFromLiteMember_noDataMember() {
        HazelcastInstance liteInst = createHazelcastInstance(smallInstanceConfig().setLiteMember(true));

        assertThatThrownBy(() -> liteInst.getJet().newLightJob(batchDag()).join())
                .hasRootCauseInstanceOf(JetException.class)
                .hasRootCauseMessage("No data member with version equal to the coordinator version found");
    }
}
