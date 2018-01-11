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

import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NoopHotRestartServicesTest {

    @Test
    public void testNoOpHotRestartService() {
        final NoOpHotRestartService service = new NoOpHotRestartService();
        service.backup();
        service.backup(0);
        service.getBackupTaskStatus();
        service.interruptBackupTask();
        service.interruptLocalBackupTask();
        assertFalse(service.isHotBackupEnabled());
        assertEquals(new BackupTaskStatus(BackupTaskState.NO_TASK, 0, 0), service.getBackupTaskStatus());
    }

    @Test
    public void testNoOpInternalHotRestartService() {
        final NoopInternalHotRestartService service = new NoopInternalHotRestartService();
        service.notifyExcludedMember(null);
        service.handleExcludedMemberUuids(null, null);
        service.resetHotRestartData();

        assertFalse(service.triggerForceStart());
        assertFalse(service.triggerPartialStart());
        assertFalse(service.isMemberExcluded(null, null));
        assertEquals(0, service.getExcludedMemberUuids().size());
        final ClusterHotRestartStatusDTO expected = new ClusterHotRestartStatusDTO();
        final ClusterHotRestartStatusDTO dto = service.getCurrentClusterHotRestartStatus();
        assertEquals(expected.getDataRecoveryPolicy(), dto.getDataRecoveryPolicy());
        assertEquals(expected.getHotRestartStatus(), dto.getHotRestartStatus());
        assertEquals(expected.getRemainingDataLoadTimeMillis(), dto.getRemainingDataLoadTimeMillis());
        assertEquals(expected.getRemainingValidationTimeMillis(), dto.getRemainingValidationTimeMillis());
    }
}
