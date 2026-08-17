/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.crdt.pncounter;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.MutationDisallowedException;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.crdt.AbstractCRDTBounceTest;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class PNCounterBounceTest extends AbstractCRDTBounceTest {

    private static final String TEST_PN_COUNTER_NAME = "counter";
    private static final ILogger LOGGER = Logger.getLogger(PNCounterBounceTest.class);

    private final AtomicLong assertionCounter = new AtomicLong();
    private final Random rnd = new Random();

    @Override
    protected void mutate(HazelcastInstance hazelcastInstance) {
        final int delta = rnd.nextInt(100) - 50;
        try {
            // Mutate the local replica directly so proxy retries while bouncing cannot apply the delta twice
            getCounter(hazelcastInstance).addAndGet(delta, null);
            assertionCounter.addAndGet(delta);
        } catch (MutationDisallowedException ex) {
            ignore(ex);
        }
    }

    private PNCounterImpl getCounter(HazelcastInstance hazelcastInstance) {
        if (hazelcastInstance instanceof HazelcastInstanceProxy proxy) {
            PNCounterService service = getNodeEngineImpl(proxy.getOriginal()).getService(PNCounterService.SERVICE_NAME);
            return service.getCounter(TEST_PN_COUNTER_NAME);
        }
        throw new IllegalArgumentException("Provided HazelcastInstance is not a proxy!");
    }

    @Override
    protected void assertState(final HazelcastInstance hazelcastInstance) {
        assertTrueEventually(() -> assertEquals(assertionCounter.get(), hazelcastInstance.getPNCounter(TEST_PN_COUNTER_NAME).get()));
    }

    @Override
    protected ILogger getLogger() {
        return LOGGER;
    }
}
