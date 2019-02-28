/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cp.internal;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.test.OverridePropertyRule.set;

// RU_COMPAT_3_11
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCPSubsystemRollingUpgrade_3_11_Test extends HazelcastRaftTestSupport {

    @Rule
    public final OverridePropertyRule version_3_11_rule = set(HAZELCAST_INTERNAL_OVERRIDE_VERSION, Versions.V3_11.toString());

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return new TestHazelcastFactory();
    }

    @Test
    public void whenCPDataStructureCreated_thenFailsWithUnsupportedOperationException() {
        int clusterSize = 3;
        for (int i = 0; i < clusterSize; i++) {
            factory.newHazelcastInstance(createConfig(clusterSize, clusterSize));
        }

        HazelcastInstance client = ((TestHazelcastFactory) factory).newHazelcastClient();
        CPSubsystem cpSubsystem = client.getCPSubsystem();

        expectedException.expect(UnsupportedOperationException.class);
        cpSubsystem.getAtomicLong("atomic");
    }
}
