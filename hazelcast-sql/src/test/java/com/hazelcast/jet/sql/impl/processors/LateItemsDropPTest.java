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

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.JetTestSupport.wm;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static java.util.Arrays.asList;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class LateItemsDropPTest {
    @Test
    public void when_noEventIsLate_than_successful() {
        SupplierEx<Processor> supplier = () -> new LateItemsDropP<JetEvent<Long>>(JetEvent::timestamp);

        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .input(asList(
                        wm(0L),
                        jetEvent(0L, 1L),
                        jetEvent(1L, 2L),
                        jetEvent(2L, 2L),
                        wm(3L)
                ))
                .expectOutput(asList(
                        wm(0L),
                        jetEvent(0L, 1L),
                        jetEvent(1L, 2L),
                        jetEvent(2L, 2L),
                        wm(3L)
                ));
    }

    @Test
    public void when_oneEventIsLate_than_dropEvent() {
        SupplierEx<Processor> supplier = () -> new LateItemsDropP<JetEvent<Long>>(JetEvent::timestamp);

        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .input(asList(
                        wm(0L),
                        jetEvent(0L, 1L),
                        jetEvent(1L, 2L),
                        wm(3L),
                        jetEvent(1L, 2L)
                ))
                .expectOutput(asList(
                        wm(0L),
                        jetEvent(0, 1L),
                        jetEvent(1, 2L),
                        wm(3)
                ));
    }

    @Test
    public void when_fewEventsAreLate_than_dropEvents() {
        SupplierEx<Processor> supplier = () -> new LateItemsDropP<JetEvent<Long>>(JetEvent::timestamp);

        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .input(asList(
                        wm(0L),
                        jetEvent(0L, 1L),
                        jetEvent(1L, 2L),
                        wm(3L),
                        jetEvent(2L, 2L),
                        jetEvent(1L, 3L),
                        wm(4L)
                ))
                .expectOutput(asList(
                        wm(0L),
                        jetEvent(0, 1L),
                        jetEvent(1, 2L),
                        wm(3),
                        wm(4)
                ));
    }
}
