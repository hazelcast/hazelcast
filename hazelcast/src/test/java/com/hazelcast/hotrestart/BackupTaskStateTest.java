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
