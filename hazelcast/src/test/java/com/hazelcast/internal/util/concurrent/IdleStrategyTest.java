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

package com.hazelcast.internal.util.concurrent;

import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

import static com.hazelcast.internal.util.concurrent.IdleStrategyTest.StrategyToTest.BACK_OFF;
import static com.hazelcast.internal.util.concurrent.IdleStrategyTest.StrategyToTest.BUSY_SPIN;
import static com.hazelcast.internal.util.concurrent.IdleStrategyTest.StrategyToTest.NO_OP;
import static java.util.Arrays.asList;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IdleStrategyTest {
    private static final int MAX_CALLS = 10;

    @Parameters(name = "manyToOne == {0}")
    public static Collection<Object[]> params() {
        return asList(new Object[][]{{NO_OP}, {BUSY_SPIN}, {BACK_OFF}});
    }

    @Parameter
    public StrategyToTest strategyToTest;

    private IdleStrategy idler;

    @Before
    public void setup() {
        this.idler = strategyToTest.create();
    }

    enum StrategyToTest {
        NO_OP {
            IdleStrategy create() {
                return new NoOpIdleStrategy();
            }
        },
        BUSY_SPIN {
            IdleStrategy create() {
                return new BusySpinIdleStrategy();
            }
        },
        BACK_OFF {
            IdleStrategy create() {
                return new BackoffIdleStrategy(1, 1, 1, 2);
            }
        };

        abstract IdleStrategy create();
    }

    @Test
    public void when_idle_thenReturnTrueEventually() {
        for (int i = 0; i < MAX_CALLS; i++) {
            if (idler.idle(i)) {
                return;
            }
        }
        fail();
    }
}
