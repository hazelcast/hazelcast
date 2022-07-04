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

package com.hazelcast.persistence;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BackupTaskStatusTest {

    private BackupTaskStatus backupTaskStatus;
    private BackupTaskStatus backupTaskStatusWithSameAttributes;

    private BackupTaskStatus backupTaskStatusOtherState;
    private BackupTaskStatus backupTaskStatusOtherCompleted;
    private BackupTaskStatus backupTaskStatusOtherTotal;

    @Before
    public void setUp() {
        backupTaskStatus = new BackupTaskStatus(BackupTaskState.SUCCESS, 5, 5);
        backupTaskStatusWithSameAttributes = new BackupTaskStatus(BackupTaskState.SUCCESS, 5, 5);

        backupTaskStatusOtherState = new BackupTaskStatus(BackupTaskState.FAILURE, 5, 5);
        backupTaskStatusOtherCompleted = new BackupTaskStatus(BackupTaskState.SUCCESS, 4, 5);
        backupTaskStatusOtherTotal = new BackupTaskStatus(BackupTaskState.SUCCESS, 5, 3);
    }

    @Test
    public void testGetProgress() {
        assertEquals(1.0f, new BackupTaskStatus(BackupTaskState.SUCCESS, 10, 10).getProgress(), 0.1);
        assertEquals(0.5f, new BackupTaskStatus(BackupTaskState.SUCCESS, 5, 10).getProgress(), 0.1);
        assertEquals(0.0f, new BackupTaskStatus(BackupTaskState.SUCCESS, 0, 10).getProgress(), 0.1);
    }

    @Test
    public void testEquals() {
        assertEquals(backupTaskStatus, backupTaskStatus);
        assertEquals(backupTaskStatus, backupTaskStatusWithSameAttributes);

        assertNotEquals(backupTaskStatus, null);
        assertNotEquals(backupTaskStatus, new Object());

        assertNotEquals(backupTaskStatus, backupTaskStatusOtherState);
        assertNotEquals(backupTaskStatus, backupTaskStatusOtherCompleted);
        assertNotEquals(backupTaskStatus, backupTaskStatusOtherTotal);
    }

    @Test
    public void testHashCode() {
        assertEquals(backupTaskStatus.hashCode(), backupTaskStatus.hashCode());
        assertEquals(backupTaskStatus.hashCode(), backupTaskStatusWithSameAttributes.hashCode());

        assumeDifferentHashCodes();
        assertNotEquals(backupTaskStatus.hashCode(), backupTaskStatusOtherState.hashCode());
        assertNotEquals(backupTaskStatus.hashCode(), backupTaskStatusOtherCompleted.hashCode());
        assertNotEquals(backupTaskStatus.hashCode(), backupTaskStatusOtherTotal.hashCode());
    }
}
