/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ThreadUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ThreadUtil.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateCurrentThreadPoolName_whenTargetPoolNameIsNull_ShouldThrowException() {
        ThreadUtil.updateCurrentThreadPoolName(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateCurrentThreadPoolName_whenTargetPoolNameIsEmpty_ShouldThrowException() {
        ThreadUtil.updateCurrentThreadPoolName("");
    }

    @Test
    public void testUpdateCurrentThreadPoolName_whenCurrentThreadNameDoesNotMatchPatternWithPoolName_ShouldNotUpdate() {
        Thread.currentThread().setName("threadName");
        assertEquals("threadName", ThreadUtil.updateCurrentThreadPoolName("targetPoolName"));
        assertEquals("threadName", Thread.currentThread().getName());

        Thread.currentThread().setName("hz.hzName.thread-1");
        assertEquals("hz.hzName.thread-1", ThreadUtil.updateCurrentThreadPoolName("targetPoolName"));
        assertEquals("hz.hzName.thread-1", Thread.currentThread().getName());
    }

    @Test
    public void testUpdateCurrentThreadPoolName_whenCurrentThreadNameMatchesPatternWithPoolName_ShouldUpdate() {
        Thread.currentThread().setName("hz.hzName.poolName.thread-1");
        assertEquals("hz.hzName.poolName.thread-1", ThreadUtil.updateCurrentThreadPoolName("targetPoolName"));
        assertEquals("hz.hzName.targetPoolName.thread-1", Thread.currentThread().getName());
    }
}
