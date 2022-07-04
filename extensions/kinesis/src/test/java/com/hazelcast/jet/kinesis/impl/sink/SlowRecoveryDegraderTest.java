/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis.impl.sink;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SlowRecoveryDegraderTest {

    private static final int K = 10;
    private static final Integer[] OUTPUTS = {500, 400, 300, 200, 100};

    private SlowRecoveryDegrader<Integer> degrader = new SlowRecoveryDegrader<>(K, OUTPUTS);

    @Test
    public void oneErrorAndBack() {
        assertOutput(500);

        degrader.error();
        assertOutput(400);

        for (int i = 0; i < K - 1; i++) {
            degrader.ok();
            assertOutput(400);
        }

        degrader.ok();
        assertOutput(500);
    }

    @Test
    public void twoErrorsAndBack() {
        assertOutput(500);

        degrader.error();
        assertOutput(400);

        degrader.error();
        assertOutput(300);

        for (int i = 0; i < K - 1; i++) {
            degrader.ok();
            assertOutput(300);
        }

        for (int i = 0; i < K; i++) {
            degrader.ok();
            assertOutput(400);
        }

        degrader.ok();
        assertOutput(500);
    }

    @Test
    public void lotsOfErrors() {
        assertOutput(500);

        degrader.error();
        assertOutput(400);

        degrader.error();
        assertOutput(300);

        degrader.error();
        assertOutput(200);

        for (int i = 0; i < OUTPUTS.length; i++) {
            degrader.error();
            assertOutput(100);
        }
    }

    @Test
    public void backAndForth() {
        assertOutput(500);

        degrader.error();
        assertOutput(400);

        degrader.ok();
        degrader.ok();
        degrader.ok();
        assertOutput(400);

        degrader.error();
        assertOutput(300);

        degrader.error();
        assertOutput(200);

        degrader.ok();
        degrader.ok();
        degrader.ok();
        assertOutput(200);

        degrader.error();
        assertOutput(100);

        degrader.error();
        assertOutput(100);

        for (int i = 0; i < K - 1; i++) {
            degrader.ok();
            assertOutput(100);
        }

        degrader.ok();
        assertOutput(200);
    }

    private void assertOutput(int output) {
        assertEquals(output, degrader.output().intValue());
    }
}
