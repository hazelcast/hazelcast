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

package com.hazelcast.config.tpc;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.tpc.TpcConfigAccessors.getEventloopCount;
import static com.hazelcast.config.tpc.TpcConfigAccessors.isTpcEnabled;
import static com.hazelcast.internal.tpc.TpcServerBootstrap.TPC_ENABLED;
import static com.hazelcast.internal.tpc.TpcServerBootstrap.TPC_EVENTLOOP_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TpcConfigTest extends HazelcastTestSupport {
    private final Config config = smallInstanceConfig();

    @Test
    public void testTPCDisabledByDefault() {
        assertFalse(isTpcEnabled(createHazelcastInstance(config)));
    }

    @Test
    public void testEventloopCountDefault() {
        config.getTpcConfig().setEnabled(true);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertTrue(isTpcEnabled(hz));
        assertEquals(Runtime.getRuntime().availableProcessors(), getEventloopCount(hz));
    }

    @Test
    public void testEventloopCount() {
        config.getTpcConfig().setEnabled(true).setEventloopCount(7);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertTrue(isTpcEnabled(hz));
        assertEquals(7, getEventloopCount(hz));
    }

    @Test
    public void testConfigValidation() {
        TpcConfig tpcConfig = config.getTpcConfig();
        assertThrows(IllegalArgumentException.class, () -> tpcConfig.setEventloopCount(0));
    }

    @Test
    public void testSystemProperties() {
        config.getTpcConfig().setEnabled(false).setEventloopCount(7);
        System.setProperty(TPC_ENABLED.getName(), "true");
        System.setProperty(TPC_EVENTLOOP_COUNT.getName(), "3");
        HazelcastInstance hz = createHazelcastInstance(config);
        assertTrue(isTpcEnabled(hz));
        assertEquals(3, getEventloopCount(hz));
    }

    @Test
    public void testConfigProperties() {
        config.getTpcConfig().setEnabled(false).setEventloopCount(7);
        config.setProperty(TPC_ENABLED.getName(), "true");
        config.setProperty(TPC_EVENTLOOP_COUNT.getName(), "3");
        HazelcastInstance hz = createHazelcastInstance(config);
        assertTrue(isTpcEnabled(hz));
        assertEquals(3, getEventloopCount(hz));
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(TpcConfig.class)
                .usingGetClass()
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }
}
