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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ChangeClusterStateTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "from:{0} to:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {ClusterState.ACTIVE, ClusterState.FROZEN},
                {ClusterState.ACTIVE, ClusterState.NO_MIGRATION},
                {ClusterState.ACTIVE, ClusterState.PASSIVE},
                {ClusterState.NO_MIGRATION, ClusterState.ACTIVE},
                {ClusterState.NO_MIGRATION, ClusterState.FROZEN},
                {ClusterState.NO_MIGRATION, ClusterState.PASSIVE},
                {ClusterState.FROZEN, ClusterState.ACTIVE},
                {ClusterState.FROZEN, ClusterState.NO_MIGRATION},
                {ClusterState.FROZEN, ClusterState.PASSIVE},
                {ClusterState.PASSIVE, ClusterState.ACTIVE},
                {ClusterState.PASSIVE, ClusterState.NO_MIGRATION},
                {ClusterState.PASSIVE, ClusterState.FROZEN},
        });
    }

    @Parameterized.Parameter(0)
    public ClusterState from;

    @Parameterized.Parameter(1)
    public ClusterState to;

    @Test
    public void changeClusterState() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(from);
        assertClusterState(from, instances);

        hz.getCluster().changeClusterState(to);
        assertClusterState(to, instances);
    }

}
