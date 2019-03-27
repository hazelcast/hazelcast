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

package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TimeUtilTest extends HazelcastTestSupport {

    @Test
    public void compareLeftTimeIsBeforeSameTimeUnit() {
        long leftTime = 2;
        TimeUnit leftTimeUnit = TimeUnit.SECONDS;

        long rightTime = 3;
        TimeUnit rightTimeUnit = TimeUnit.SECONDS;

        assertEquals(-1, TimeUtil.compare(leftTime, leftTimeUnit, rightTime, rightTimeUnit));
    }

    @Test
    public void compareLeftTimeIsBeforeDifferentTimeUnit() {
        long leftTime = 2;
        TimeUnit leftTimeUnit = TimeUnit.SECONDS;

        long rightTime = 1;
        TimeUnit rightTimeUnit = TimeUnit.DAYS;

        assertEquals(-1, TimeUtil.compare(leftTime, leftTimeUnit, rightTime, rightTimeUnit));
    }

    @Test
    public void compareLeftTimeIsEqualSameTimeUnit() {
        long leftTime = 2;
        TimeUnit leftTimeUnit = TimeUnit.SECONDS;

        long rightTime = 2;
        TimeUnit rightTimeUnit = TimeUnit.SECONDS;

        assertEquals(0, TimeUtil.compare(leftTime, leftTimeUnit, rightTime, rightTimeUnit));
    }

    @Test
    public void compareLeftTimeIsEqualDifferentTimeUnit() {
        long leftTime = 60;
        TimeUnit leftTimeUnit = TimeUnit.SECONDS;

        long rightTime = 1;
        TimeUnit rightTimeUnit = TimeUnit.MINUTES;

        assertEquals(0, TimeUtil.compare(leftTime, leftTimeUnit, rightTime, rightTimeUnit));
    }

    @Test
    public void compareLeftTimeIsAfterSameTimeUnit() {
        long leftTime = 3;
        TimeUnit leftTimeUnit = TimeUnit.SECONDS;

        long rightTime = 2;
        TimeUnit rightTimeUnit = TimeUnit.SECONDS;

        assertEquals(1, TimeUtil.compare(leftTime, leftTimeUnit, rightTime, rightTimeUnit));
    }

    @Test
    public void compareLeftTimeIsAfterDifferentTimeUnit() {
        long leftTime = 61;
        TimeUnit leftTimeUnit = TimeUnit.SECONDS;

        long rightTime = 1;
        TimeUnit rightTimeUnit = TimeUnit.MINUTES;

        assertEquals(1, TimeUtil.compare(leftTime, leftTimeUnit, rightTime, rightTimeUnit));
    }

    @Test(expected = NullPointerException.class)
    public void compareLeftTimeUnitIsNull() {
        long leftTime = 61;
        TimeUnit leftTimeUnit = null;

        long rightTime = 1;
        TimeUnit rightTimeUnit = TimeUnit.MINUTES;

        assertEquals(1, TimeUtil.compare(leftTime, leftTimeUnit, rightTime, rightTimeUnit));
    }

    @Test(expected = NullPointerException.class)
    public void compareRightTimeUnitIsNull() {
        long leftTime = 61;
        TimeUnit leftTimeUnit = TimeUnit.SECONDS;

        long rightTime = 1;
        TimeUnit rightTimeUnit = null;

        assertEquals(1, TimeUtil.compare(leftTime, leftTimeUnit, rightTime, rightTimeUnit));
    }
}
