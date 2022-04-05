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

package com.hazelcast.jet.core;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.impl.execution.DoneItem;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.Repeat;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class ObservableRingbufferContainerLeakTest extends JetTestSupport {

    /**
     * Regression test for #19580
     */
    @Test
    @Repeat(100)
    public void testLeakingRingbufferContainerWhenUsingObservableIterator() {
        Config config = smallInstanceConfig();
        config.setProperty("hazelcast.partition.count", "1");
        config.getJetConfig().setEnabled(true);

        HazelcastInstance hz = createHazelcastInstance(config);
        JetService jet = hz.getJet();

        Ringbuffer<Object> ringbuffer = hz.getRingbuffer("__jet.observables.my-observable");

        Observable<Object> obs = jet.getObservable("my-observable");
        ringbuffer.add(42);
        ringbuffer.addAsync(DoneItem.DONE_ITEM, OverflowPolicy.OVERWRITE);

        Iterator<Object> it = obs.iterator();
        it.hasNext();
        it.next();
        it.hasNext();

        obs.destroy();

        RingbufferService ringbufferService = Accessors.getService(hz, RingbufferService.SERVICE_NAME);

        Map<ObjectNamespace, RingbufferContainer> containers = ringbufferService
                .getContainers()
                .values()
                .iterator()
                .next(); // We set partition count to 1 so there will be only 1 item

        assertThat(containers).hasSize(0);
    }
}
