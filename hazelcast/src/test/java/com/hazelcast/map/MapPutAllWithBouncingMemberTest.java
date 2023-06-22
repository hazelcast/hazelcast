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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.HashMap;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class MapPutAllWithBouncingMemberTest extends HazelcastTestSupport {

    private static final int MAP_SIZE = 1000;
    private static final int DURATION_SECONDS = 30;

    @Parameterized.Parameter
    public int putAllBatchSize;

    @Parameterized.Parameters(name = "putAllBatchSize: {0}")
    public static Collection<Integer> data() {
        return asList(0, 2);
    }

    @Override
    protected Config getConfig() {
        Config cfg = super.getConfig();
        if (putAllBatchSize > 0) {
            cfg.setProperty("hazelcast.map.put.all.batch.size", String.valueOf(putAllBatchSize));
        }
        return cfg;
    }

    @Rule
    public BounceMemberRule bounceMemberRule =
            BounceMemberRule.with(() -> getConfig())
                    .driverType(BounceTestConfiguration.DriverType.MEMBER)
                    .clusterSize(2)
                    .driverCount(2)
                    .build();

    @Test
    public void testPutAll_whenAddingAndTerminatingMembers_thenPutAllShouldNotFail() {
        final IMap<Integer, Integer> map = bounceMemberRule.getSteadyMember().getMap(randomMapName());

        HashMap<Integer, Integer> batch = new HashMap<>();
        for (int i = 0; i < MAP_SIZE; i++) {
            batch.put(i, i);
        }

        bounceMemberRule.testRepeatedly(new Runnable[]{
                () -> {
                    map.clear();
                    map.putAll(batch);
                    sleepMillis(3);
                }
        }, DURATION_SECONDS);

        assertEquals("The map size should be " + MAP_SIZE, MAP_SIZE, map.size());
    }
}
