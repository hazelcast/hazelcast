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

package com.hazelcast.jet.impl;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.metrics.MetricsResultSet;
import org.junit.Test;

import java.util.stream.StreamSupport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JetClientInstanceImplTest extends JetTestSupport {

    @Test
    public void test() {
        JetConfig cfg = new JetConfig();
        cfg.getMetricsConfig().setEnabled(true);
        JetInstance inst = createJetMember(cfg);
        JetClientInstanceImpl client = (JetClientInstanceImpl) createJetClient();
        assertTrueEventually(() -> {
            ICompletableFuture<MetricsResultSet> resultFuture =
                    client.readMetricsAsync(inst.getHazelcastInstance().getCluster().getLocalMember(), 0);
            MetricsResultSet result = resultFuture.get();
            assertFalse(result.collections().isEmpty());
            assertTrue(StreamSupport.stream(result.collections().get(0).spliterator(), false)
                         .anyMatch(m -> m.key().equals("[metric=os.processCpuLoad]")));
        }, 5);
    }
}
