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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.version.MemberVersion;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class ReadMetricsTest extends JetTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_readMetricsAsync() {
        JetInstance inst = createJetMember();
        JetClientInstanceImpl client = (JetClientInstanceImpl) createJetClient();
        assertTrueEventually(() -> {
            Member member = inst.getHazelcastInstance().getCluster().getLocalMember();
            MetricsResultSet result = client.readMetricsAsync(member, 0).get();
            assertFalse(result.collections().isEmpty());
            assertTrue(
                    StreamSupport.stream(result.collections().get(0).spliterator(), false)
                            .anyMatch(m -> m.key().equals("[metric=cluster.size]"))
            );

            // immediate next call should not return empty result
            result = client.readMetricsAsync(member, result.nextSequence()).get();
            assertFalse(result.collections().isEmpty());
        }, 30);
    }

    @Test
    public void when_invalidUUID() throws ExecutionException, InterruptedException {
        JetInstance inst = createJetMember();
        JetClientInstanceImpl client = (JetClientInstanceImpl) createJetClient();
        Address addr = inst.getCluster().getLocalMember().getAddress();
        MemberVersion ver = inst.getCluster().getLocalMember().getVersion();
        MemberImpl member = new MemberImpl(addr, ver, false, UuidUtil.newUnsecureUuidString());

        exception.expectCause(Matchers.instanceOf(IllegalArgumentException.class));
        client.readMetricsAsync(member, 0).get();
    }

    @Test
    public void when_metricsDisabled() throws ExecutionException, InterruptedException {
        JetConfig cfg = new JetConfig();
        cfg.getMetricsConfig().setEnabled(false);
        JetInstance inst = createJetMember(cfg);
        JetClientInstanceImpl client = (JetClientInstanceImpl) createJetClient();

        exception.expectCause(Matchers.instanceOf(IllegalArgumentException.class));
        MetricsResultSet resultSet = client.readMetricsAsync(inst.getCluster().getLocalMember(), 0).get();
    }
}
