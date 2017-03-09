/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CardinalityEstimatorConfigTest extends HazelcastTestSupport {

    private CardinalityEstimatorConfig config = new CardinalityEstimatorConfig();

    @Test
    public void testConstructor_withName() {
        config = new CardinalityEstimatorConfig("myEstimator");

        assertEquals("myEstimator", config.getName());
    }

    @Test
    public void testConstructor_withNameAndBackupCounts() {
        config = new CardinalityEstimatorConfig("myEstimator", 2, 3);

        assertEquals("myEstimator", config.getName());
        assertEquals(2, config.getBackupCount());
        assertEquals(3, config.getAsyncBackupCount());
        assertEquals(5, config.getTotalBackupCount());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_withNameAndBackupCounts_withNegativeBackupCount() {
        new CardinalityEstimatorConfig("myEstimator", -1, 3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_withNameAndBackupCounts_withNegativeAsyncBackupCount() {
        new CardinalityEstimatorConfig("myEstimator", 2, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_withNameAndBackupCounts_withNegativeBackupSum1() {
        new CardinalityEstimatorConfig("myEstimator", 6, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_withNameAndBackupCounts_withNegativeBackupSum2() {
        new CardinalityEstimatorConfig("myEstimator", 1, 6);
    }

    @Test
    public void testSetName() {
        config.setName("myCardinalityEstimator");

        assertEquals("myCardinalityEstimator", config.getName());
    }

    @Test
    public void testBackupCount() {
        config.setBackupCount(2);
        config.setAsyncBackupCount(3);

        assertEquals(2, config.getBackupCount());
        assertEquals(3, config.getAsyncBackupCount());
        assertEquals(5, config.getTotalBackupCount());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBackupCount_withNegativeValue() {
        config.setBackupCount(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetAsyncBackupCount_withNegativeValue() {
        config.setAsyncBackupCount(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBackupCount_withInvalidValue() {
        config.setBackupCount(7);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetAsyncBackupCount_withInvalidValue() {
        config.setAsyncBackupCount(7);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBackupCount_withInvalidBackupSum() {
        config.setAsyncBackupCount(1);
        config.setBackupCount(6);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetAsyncBackupCount_withInvalidBackupSum() {
        config.setBackupCount(1);
        config.setAsyncBackupCount(6);
    }

    @Test
    public void testToString() {
        assertContains(config.toString(), "CardinalityEstimatorConfig");
    }
}
