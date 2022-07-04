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

package com.hazelcast.jet.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InstanceConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void when_NegativeBackupCount_thenThrowsException() {
        // When
        InstanceConfig instanceConfig = new InstanceConfig();

        // Then
        expectedException.expect(IllegalArgumentException.class);
        instanceConfig.setBackupCount(-1);
    }

    @Test
    public void when_TooBigBackupCount_thenThrowsException() {
        // When
        InstanceConfig instanceConfig = new InstanceConfig();

        // Then
        expectedException.expect(IllegalArgumentException.class);
        instanceConfig.setBackupCount(10);
    }

    @Test
    public void when_setBackupCount_thenReturnsBackupCount() {
        // When
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setBackupCount(3);

        // Then
        assertEquals(3, instanceConfig.getBackupCount());
    }

    @Test
    public void when_setThreadCount_thenReturnsThreadCount() {
        // When
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setCooperativeThreadCount(5);

        // Then
        assertEquals(5, instanceConfig.getCooperativeThreadCount());
    }
    @Test
    public void when_setFlowControlMs_thenReturnsFlowControlMs() {
        // When
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFlowControlPeriodMs(500);

        // Then
        assertEquals(500, instanceConfig.getFlowControlPeriodMs());
    }

    @Test
    public void when_scaleUpDelay_then_returnsDelay() {
        // When
        InstanceConfig config = new InstanceConfig();
        config.setScaleUpDelayMillis(123);

        // Then
        assertEquals(123L, config.getScaleUpDelayMillis());
    }

    @Test
    public void when_losslessRestartEnabled_then_returnsEnabled() {
        // When
        InstanceConfig config = new InstanceConfig();
        config.setLosslessRestartEnabled(true);

        // Then
        assertTrue(config.isLosslessRestartEnabled());
    }
}
