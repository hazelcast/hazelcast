package com.hazelcast.jet.impl.jobupload;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JobUploadStoreTest {
    @Spy
    ConcurrentHashMap<UUID, JobUploadStatus> jobMap;
    @InjectMocks
    JobUploadStore jobUploadStore;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testCleanExpiredUploads() {
        UUID sessionID = UUID.randomUUID();
        JobUploadStatus jobUploadStatus = new JobUploadStatus(sessionID, new RunJarParameterObject());
        jobUploadStatus.lastUpdatedTime = System.nanoTime() - TimeUnit.MINUTES.toNanos(JobUploadStatus.EXPIRATION_MINUTES);

        jobMap.put(sessionID, jobUploadStatus);
        jobUploadStore.cleanExpiredUploads();

        assertEquals(0, jobMap.size());
    }

    @Test
    public void testRemove() throws Exception {
        try {
            UUID sessionID = UUID.randomUUID();
            jobUploadStore.remove(sessionID);
        } catch (Exception e) {
            fail("Test sould not throw");
        }
    }

    @Test
    public void testProcessJarMetaData() {
        UUID sessionID = UUID.randomUUID();
        RunJarParameterObject parameterObject = new RunJarParameterObject();
        jobUploadStore.processJarMetaData(sessionID, parameterObject);

        assertEquals(1, jobMap.size());

    }

    @Test
    public void testProcessJarData() throws Exception {
        UUID sessionID = UUID.randomUUID();
        RunJarParameterObject parameterObject = new RunJarParameterObject();
        jobUploadStore.processJarMetaData(sessionID, parameterObject);

        boolean result = jobUploadStore.processJarData(sessionID, 1, 2, new byte[]{(byte) 0});
        assertFalse(result);

        result = jobUploadStore.processJarData(sessionID, 2, 2, new byte[]{(byte) 0});
        assertTrue(result);
        assertTrue(Files.exists(parameterObject.getJarPath()));

        jobUploadStore.remove(sessionID);

        assertFalse(Files.exists(parameterObject.getJarPath()));

    }
}
