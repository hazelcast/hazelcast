/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.tpc.TpcServerBootstrap.TPC_ENABLED;
import static com.hazelcast.test.OverridePropertyRule.clear;
import static org.junit.Assert.assertThrows;


@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TpcConfigIntegrationTest {

    @Rule
    public final OverridePropertyRule ruleSysPropTpcEnabled = clear(TPC_ENABLED.getName());

    @Test
    public void whenTpcEnabled_fromSystemProperty() {
        System.setProperty(TPC_ENABLED.getName(), "true");

        assertThrows(IllegalStateException.class, Hazelcast::newHazelcastInstance);
    }

    @Test
    public void whenTpcEnabled_fromConfig() {
        Config config = new Config();
        config.getTpcConfig().setEnabled(true);

        assertThrows(IllegalStateException.class, () -> Hazelcast.newHazelcastInstance(config));
    }
}
