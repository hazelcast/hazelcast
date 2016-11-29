package com.hazelcast.monitor.impl;

import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.hotrestart.BackupTaskStatus;
import com.hazelcast.monitor.HotRestartState;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartStateImplTest {

    @Test
    public void testSerializationAndDeserizalization() throws Exception {
        final BackupTaskStatus backupTaskStatus = new BackupTaskStatus(BackupTaskState.IN_PROGRESS, 5, 10);
        final HotRestartState state = new HotRestartStateImpl(backupTaskStatus);
        final HotRestartStateImpl deserialized = new HotRestartStateImpl();
        deserialized.fromJson(state.toJson());

        assertEquals(backupTaskStatus, deserialized.getBackupTaskStatus());
    }

}
