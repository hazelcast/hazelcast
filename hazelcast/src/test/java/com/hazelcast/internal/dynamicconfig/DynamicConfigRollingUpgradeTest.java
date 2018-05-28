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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.config.CountDownLatchConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SerializationSamplesExcluded;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.internal.cluster.Versions.V3_8;
import static com.hazelcast.internal.cluster.Versions.V3_9;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class, SerializationSamplesExcluded.class})
public class DynamicConfigRollingUpgradeTest extends HazelcastTestSupport {

    @Test(expected = UnsupportedOperationException.class)
    public void testThrowsExceptionWhenRunningInClusterVersion38() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance(V3_8);
        hazelcastInstance.getConfig().addMapConfig(new MapConfig(randomName()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testThrowsExceptionWhenAddingAtomicLongConfigInClusterVersion39() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance(V3_9);
        hazelcastInstance.getConfig().addAtomicLongConfig(new AtomicLongConfig(randomMapName()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testThrowsExceptionWhenAddingAtomicReferenceConfigInClusterVersion39() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance(V3_9);
        hazelcastInstance.getConfig().addAtomicReferenceConfig(new AtomicReferenceConfig(randomMapName()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testThrowsExceptionWhenAddingCountDownLatchConfigInClusterVersion39() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance(V3_9);
        hazelcastInstance.getConfig().addCountDownLatchConfig(new CountDownLatchConfig(randomMapName()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testThrowsExceptionWhenAddingFlakeIdConfigInClusterVersion39() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance(V3_9);
        hazelcastInstance.getConfig().addFlakeIdGeneratorConfig(new FlakeIdGeneratorConfig(randomName()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testThrowsExceptionWhenAddingPNCounterConfigInClusterVersion39() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance(V3_9);
        hazelcastInstance.getConfig().addPNCounterConfig(new PNCounterConfig(randomName()));
    }

    protected HazelcastInstance createHazelcastInstance(Version version) {
        // system properties are cleared automatically by the Hazelcast Runner
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, version.toString());
        return super.createHazelcastInstance();
    }
}
