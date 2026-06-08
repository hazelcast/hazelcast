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

package com.hazelcast.vector.impl.migration;

import com.hazelcast.config.Config;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import com.hazelcast.vector.VectorValues;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.vector.impl.VectorTestUtils.randomVec;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public abstract class VectorCollectionSearchBounceTestBase extends HazelcastTestSupport {

    protected static final String COLLECTION_NAME = "vectors";
    protected static final int DIMENSION = 10;
    protected static final int PARTITION_COUNT = 11;
    protected static final int INITIAL_SIZE = 200_000;
    protected static final int INITIAL_PARTITION_SIZE = INITIAL_SIZE / PARTITION_COUNT;
    protected static final int CONCURRENCY = 10;

    // the probabilities are relatively high, but they entries are distributed among partitions
    // (by default 11 in this test) so final duplicate vector probability is lower.
    private static final double DUPLICATE_REUSE_PROBABILITY = 0.5;
    private static final double DUPLICATE_STORE_PROBABILITY = 0.5;

    @Rule
    public BounceMemberRule bounceMemberRule =
            BounceMemberRule.with(this::getConfig)
                    .clusterSize(4)
                    .driverCount(4)
                    .driverType(getDriverType())
                    .useTerminate(useTerminate())
                    .avoidOverlappingTerminations(true)
                    .build();

    @Parameterized.Parameter
    public boolean deduplicate;

    @Parameterized.Parameters(name = "deduplication={0}")
    public static List<Object[]> parameters() {
        return cartesianProduct(List.of(false, true));
    }

    protected Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
    }

    // XXX: It's better to use parametrized test here, but parameters don't play
    // nice with rules: parameters are injected after rules instantiation.
    protected boolean useTerminate() {
        return false;
    }

    protected BounceTestConfiguration.DriverType getDriverType() {
        return BounceTestConfiguration.DriverType.LITE_MEMBER;
    }

    protected AtomicInteger nextKey = new AtomicInteger();
    protected AtomicReference<VectorValues> recentVector = new AtomicReference<>(randomVec(DIMENSION));

    /**
     * Generates a new vector. If the test config requests it, sometimes produces duplicates.
     */
    protected VectorValues generateVector() {
        boolean reuse = deduplicate && ThreadLocalRandom.current().nextDouble() > DUPLICATE_REUSE_PROBABILITY;
        boolean store = deduplicate && ThreadLocalRandom.current().nextDouble() > DUPLICATE_STORE_PROBABILITY;
        var newVec = reuse ? recentVector.get() : randomVec(DIMENSION);
        if (store && !reuse) {
            recentVector.set(newVec);
        }
        return newVec;
    }
}
