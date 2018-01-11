/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.hotrestart;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BackupTaskStateTest {

    @Test
    public void testBackupState() throws Exception {
        assertFalse(BackupTaskState.NO_TASK.isDone());
        assertFalse(BackupTaskState.NOT_STARTED.isDone());
        assertFalse(BackupTaskState.IN_PROGRESS.isDone());
        assertTrue(BackupTaskState.FAILURE.isDone());
        assertTrue(BackupTaskState.SUCCESS.isDone());


        assertFalse(BackupTaskState.NO_TASK.inProgress());
        assertTrue(BackupTaskState.NOT_STARTED.inProgress());
        assertTrue(BackupTaskState.IN_PROGRESS.inProgress());
        assertFalse(BackupTaskState.FAILURE.inProgress());
        assertFalse(BackupTaskState.SUCCESS.inProgress());
    }
}
