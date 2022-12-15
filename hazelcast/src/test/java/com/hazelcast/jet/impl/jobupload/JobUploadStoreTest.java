package com.hazelcast.jet.impl.jobupload;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.nio.file.Files;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

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
        JobUploadStatus jobUploadStatus;

        //Mock static method of Instant.now() to return an expired time
        Instant expiredTime = Instant.now().minus(1, ChronoUnit.MINUTES);
        try (MockedStatic<Instant> mocked = mockStatic(Instant.class)) {
            Instant Instant = mock(Instant.class);
            when(Instant.now()).thenReturn(expiredTime);

            RunJarParameterObject runJarParameterObject = new RunJarParameterObject();
            jobUploadStatus = new JobUploadStatus(runJarParameterObject);
        }

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
        parameterObject.setSessionId(sessionID);
        jobUploadStore.processJarMetaData(parameterObject);

        assertEquals(1, jobMap.size());

    }

    @Test
    public void testProcessJarData() throws Exception {
        UUID sessionID = UUID.randomUUID();
        RunJarParameterObject parameterObject = new RunJarParameterObject();
        parameterObject.setSessionId(sessionID);
        jobUploadStore.processJarMetaData(parameterObject);

        byte[] jarData = {(byte) 0};
        RunJarParameterObject result = jobUploadStore.processJarData(sessionID, 1, 2, jarData, jarData.length);
        assertNull(result);

        result = jobUploadStore.processJarData(sessionID, 2, 2, jarData, jarData.length);
        assertNotNull(result);
        assertTrue(Files.exists(parameterObject.getJarPath()));

        jobUploadStore.remove(sessionID);

        assertFalse(Files.exists(parameterObject.getJarPath()));

    }
}
