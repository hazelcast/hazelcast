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

package com.hazelcast.internal.serialization.impl.compact.schema;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import com.hazelcast.test.bounce.DriverFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class CompactSchemaReplicationBouncingTest extends HazelcastTestSupport {

    private static final int DURATION_SECONDS = 30;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
            .clusterSize(4)
            .driverCount(1)
            .useTerminate(true)
            .driverType(getDriverType())
            .driverFactory(getDriverFactory())
            .build();

    @Test
    public void testSchemaReplicatedSuccessfully() {
        HazelcastInstance driver = bounceMemberRule.getNextTestDriver();
        NewSchemaRegisterer runnable = new NewSchemaRegisterer(driver);
        bounceMemberRule.testRepeatedly(new Runnable[]{runnable}, DURATION_SECONDS);

        int registeredSchemaCount = runnable.getCounterValue();
        HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
        Collection<Schema> schemas = TestUtil.getNode(steadyMember).getSchemaService().getAllSchemas();
        assertEquals(registeredSchemaCount, schemas.size());
    }

    protected DriverFactory getDriverFactory() {
        return null;
    }

    protected BounceTestConfiguration.DriverType getDriverType() {
        return BounceTestConfiguration.DriverType.MEMBER;
    }

    public Config getConfig() {
        return smallInstanceConfig();
    }

    static class NewSchemaRegisterer implements Runnable {

        private final AtomicInteger counter = new AtomicInteger(0);
        private final HazelcastInstance driver;

        NewSchemaRegisterer(HazelcastInstance driver) {
            this.driver = driver;
        }

        @Override
        public void run() {
            int value = counter.getAndIncrement();

            // Each compact object will have a different schema
            // due to different field names
            GenericRecord record = GenericRecordBuilder.compact("foo")
                    .setInt32(Integer.toString(value), value)
                    .build();

            driver.getMap("foo").set(value, record);
        }

        public int getCounterValue() {
            return counter.get();
        }
    }
}
