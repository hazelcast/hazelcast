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

package com.hazelcast.internal.util;

import org.junit.Assert;
import org.junit.Test;

import static com.hazelcast.internal.partition.IPartition.MAX_BACKUP_COUNT;
import static org.junit.Assert.fail;

public class BackupPreconditionsTest {

    @Test
    public void checkBackupCount() {
        checkBackupCount(-1, 0, false);
        checkBackupCount(-1, -1, false);
        checkBackupCount(0, -1, false);
        checkBackupCount(0, 0, true);
        checkBackupCount(0, 1, true);
        checkBackupCount(1, 1, true);
        checkBackupCount(2, 1, true);
        checkBackupCount(1, 2, true);
        checkBackupCount(MAX_BACKUP_COUNT, 0, true);
        checkBackupCount(0, MAX_BACKUP_COUNT, true);
        checkBackupCount(MAX_BACKUP_COUNT, 1, false);
        checkBackupCount(MAX_BACKUP_COUNT + 1, 0, false);
        checkBackupCount(0, MAX_BACKUP_COUNT + 1, false);
    }

    public void checkBackupCount(int newBackupCount, int currentAsyncBackupCount, boolean success) {
        if (success) {
            int result = BackupPreconditions.checkBackupCount(newBackupCount, currentAsyncBackupCount);
            Assert.assertEquals(result, newBackupCount);
        } else {
            try {
                BackupPreconditions.checkBackupCount(newBackupCount, currentAsyncBackupCount);
                fail();
            } catch (IllegalArgumentException expected) {
            }
        }
    }

    @Test
    public void checkAsyncBackupCount() {
        checkAsyncBackupCount(-1, 0, false);
        checkAsyncBackupCount(-1, -1, false);
        checkAsyncBackupCount(0, -1, false);
        checkAsyncBackupCount(0, 0, true);
        checkAsyncBackupCount(0, 1, true);
        checkAsyncBackupCount(1, 1, true);
        checkAsyncBackupCount(2, 1, true);
        checkAsyncBackupCount(1, 2, true);
        checkAsyncBackupCount(MAX_BACKUP_COUNT, 0, true);
        checkAsyncBackupCount(0, MAX_BACKUP_COUNT, true);
        checkAsyncBackupCount(MAX_BACKUP_COUNT, 1, false);
        checkAsyncBackupCount(MAX_BACKUP_COUNT + 1, 0, false);
        checkAsyncBackupCount(0, MAX_BACKUP_COUNT + 1, false);
    }

    public void checkAsyncBackupCount(int currentBackupCount, int newAsyncBackupCount, boolean success) {
        if (success) {
            int result = BackupPreconditions.checkAsyncBackupCount(currentBackupCount, newAsyncBackupCount);
            Assert.assertEquals(result, newAsyncBackupCount);
        } else {
            try {
                BackupPreconditions.checkAsyncBackupCount(currentBackupCount, newAsyncBackupCount);
                fail();
            } catch (IllegalArgumentException expected) {
            }
        }
    }

    // =====================================================

}
