/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class StoreSnapshotTaskletTest extends JetTestSupport {

    private JetInstance instance;
    private SnapshotContext ssContext;
    private MockInboundStream input;
    private StoreSnapshotTasklet sst;
    private NodeEngineImpl nodeEngine;

    @Before
    public void before() {
        instance = createJetMember();
    }

    @After
    public void after() {
        shutdownFactory();
    }

    private void init(List<Object> inputData) {
        nodeEngine = ((HazelcastInstanceImpl) instance.getHazelcastInstance()).node.nodeEngine;
        ssContext = new SnapshotContext(nodeEngine.getLogger(SnapshotContext.class), 1, 1, 1,
                ProcessingGuarantee.EXACTLY_ONCE);
        ssContext.initTaskletCount(1, 0);
        inputData = new ArrayList<>(inputData);
        // serialize input data
        for (int i = 0; i < inputData.size(); i++) {
            if (inputData.get(i) instanceof Entry) {
                Entry<?, ?> en = (Entry<?, ?>) inputData.get(i);
                inputData.set(i, entry(serialize(en.getKey()), serialize(en.getValue())));
            }
        }
        input = new MockInboundStream(0, inputData, 1);
        sst = new StoreSnapshotTasklet(ssContext, 1, input, nodeEngine, "myVertex", false);
    }

    @Test
    public void when_doneItemOnInput_then_eventuallyDone() {
        init(Collections.singletonList(DONE_ITEM));
        assertTrueEventually(() -> assertTrue("tasklet not done", sst.call().isDone()), 3);
    }

    @Test
    public void when_item_then_storedToMap() {
        init(Collections.singletonList(entry("k", "v")));
        IStreamMap<Object, Object> map = instance.getMap(sst.currMapName());
        assertTrueEventually(() -> {
            sst.call();
            assertEquals("v", map.get("k"));
        }, 3);
    }

    @Test
    public void when_barrier_then_snapshotDone() {
        init(Collections.singletonList(new SnapshotBarrier(2)));
        ssContext.startNewSnapshot(2);
        assertEquals(2, sst.pendingSnapshotId);
        assertTrueEventually(() -> {
            sst.call();
            assertEquals(3, sst.pendingSnapshotId);
        }, 3);
    }

    @Test
    public void when_itemAndBarrier_then_snapshotDone() {
        init(asList(entry("k", "v"), new SnapshotBarrier(2)));
        ssContext.startNewSnapshot(2);
        assertEquals(2, sst.pendingSnapshotId);
        IStreamMap<Object, Object> map = instance.getMap(sst.currMapName());
        assertTrueEventually(() -> {
            sst.call();
            assertEquals(3, sst.pendingSnapshotId);
            assertEquals("v", map.get("k"));
        }, 3);
    }

    private Data serialize(Object o) {
        return nodeEngine.getSerializationService().toData(o);
    }
}
