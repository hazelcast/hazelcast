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

package com.hazelcast.jet;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.exception.JetDisabledException;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JetTest extends JetTestSupport {

    @Test
    public void when_defaultMapConfig_then_notUsed() {
        // When
        Config config = smallInstanceConfig();
        config.getMapConfig("default")
                .setTimeToLiveSeconds(MapConfig.DEFAULT_TTL_SECONDS + 1);
        HazelcastInstance instance = createHazelcastInstance(config);

        // Then
        int actualTTL = instance.getConfig()
                .findMapConfig(INTERNAL_JET_OBJECTS_PREFIX + "fooMap").getTimeToLiveSeconds();
        assertEquals(MapConfig.DEFAULT_TTL_SECONDS, actualTTL);
    }

    @Test
    public void when_jetDisabled_then_getJetInstanceThrowsException() {
        // When
        Config config = smallInstanceConfig();
        config.getJetConfig().setEnabled(false);
        HazelcastInstance instance = createHazelcastInstance(config);

        // Then
        assertThrows(JetDisabledException.class, instance::getJet);
    }

    @Test
    public void when_jetDisabled_and_usingClient_then_getJetInstanceThrowsException() {
        // When
        Config config = smallInstanceConfig();
        config.getJetConfig().setEnabled(false);
        createHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1))
                .writeTo(Sinks.noop());

        assertThrows(JetDisabledException.class, () -> client.getJet().newJob(p).join());
    }

    @Test
    public void when_jetDisabled_and_usingClient_then_getSummaryListThrowsException() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setEnabled(false);
        createHazelcastInstance(config);
        HazelcastInstance client = createHazelcastClient();
        JetClientInstanceImpl jet = (JetClientInstanceImpl) client.getJet();

        assertThrows(JetDisabledException.class, jet::getJobSummaryList);
    }

    @Test
    public void when_resourceUploadDisabled_and_submitJobWithResource_then_jobFails() {
        // When
        HazelcastInstance hz = createHazelcastInstance();

        JobConfig jobConfig = new JobConfig().addClass(JetTest.class);
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1))
                .writeTo(Sinks.noop());

        // Then
        assertThrows(JetException.class, () -> hz.getJet().newJob(p, jobConfig));
    }

}
