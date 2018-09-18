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
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.metrics.mancenter.MetricsResultSet;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

import static com.hazelcast.util.ExceptionUtil.peel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
public class ReadMetricsTest extends JetTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_readMetricsAsync() throws Exception {
        JetConfig conf = new JetConfig();
        conf.getMetricsConfig().setCollectionIntervalSeconds(1);
        JetInstance instance = createJetMember(conf);
        JetClientInstanceImpl client = (JetClientInstanceImpl) createJetClient();
        Member member = instance.getHazelcastInstance().getCluster().getLocalMember();

        long nextSequence = 0;
        for (int i = 0; i < 3; i++) {
            MetricsResultSet result = client.readMetricsAsync(member, nextSequence).get();
            nextSequence = result.nextSequence();
            // call should not return empty result - it should wait until a result is available
            assertFalse("empty result", result.collections().isEmpty());
            assertTrue(
                    StreamSupport.stream(result.collections().get(0).spliterator(), false)
                                 .anyMatch(m -> m.key().equals("[metric=cluster.size]"))
            );
        }
    }

    @Test
    public void when_invalidUUID() throws ExecutionException, InterruptedException {
        JetInstance instance = createJetMember();
        JetClientInstanceImpl client = (JetClientInstanceImpl) createJetClient();
        Address addr = instance.getCluster().getLocalMember().getAddress();
        MemberVersion ver = instance.getCluster().getLocalMember().getVersion();
        MemberImpl member = new MemberImpl(addr, ver, false, UuidUtil.newUnsecureUuidString());

        exception.expectCause(Matchers.instanceOf(IllegalArgumentException.class));
        client.readMetricsAsync(member, 0).get();
    }

    @Test
    public void when_metricsDisabled() throws ExecutionException, InterruptedException {
        JetConfig cfg = new JetConfig();
        cfg.getMetricsConfig().setEnabled(false);
        JetInstance instance = createJetMember(cfg);
        JetClientInstanceImpl client = (JetClientInstanceImpl) createJetClient();

        exception.expectCause(Matchers.instanceOf(IllegalArgumentException.class));
        MetricsResultSet resultSet = client.readMetricsAsync(instance.getCluster().getLocalMember(), 0).get();
    }

    @Test
    public void when_metricsTimeout() throws ExecutionException, InterruptedException {
        JetConfig cfg = new JetConfig();
        cfg.getMetricsConfig().setCollectionIntervalSeconds(1);
        JetInstance instance = createJetMember(cfg);
        JetClientInstanceImpl client = (JetClientInstanceImpl) createJetClient();

        JetMetricsService service = getNodeEngineImpl(instance).getService(JetMetricsService.SERVICE_NAME);
        service.pauseCollection();
        AtomicLong seq = new AtomicLong(0);
        Member member = instance.getCluster().getLocalMember();
        assertTrueEventually(() -> {
            try {
                MetricsResultSet result = client.readMetricsAsync(member, seq.get()).get();
                seq.set(result.nextSequence());
                fail("readMetricsAsync call should have timed out, got "
                        + result.collections().size() + " collections instead");
            } catch (ExecutionException e) {
                Exception peeled = peel(e);
                assertInstanceOf(OperationTimeoutException.class, peeled);
            }
        }, 30);
        service.resumeCollection();
        MetricsResultSet resultSet = client.readMetricsAsync(member, seq.get()).get();
        assertEquals(1, resultSet.collections().size());
    }
}
