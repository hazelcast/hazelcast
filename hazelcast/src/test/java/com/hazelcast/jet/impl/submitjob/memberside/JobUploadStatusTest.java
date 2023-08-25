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

package com.hazelcast.jet.impl.submitjob.memberside;

import com.hazelcast.jet.JetException;
import com.hazelcast.mock.MockUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JobUploadStatusTest {

    @Mock
    Clock clock;
    @Mock
    JobMetaDataParameterObject jobMetaDataParameterObject;
    @InjectMocks
    JobUploadStatus jobUploadStatus;

    private AutoCloseable openMocks;

    @Before
    public void setUp() {
        openMocks = openMocks(this);
    }

    @After
    public void cleanUp() {
        MockUtil.closeMocks(openMocks);
    }

    @Test
    public void testIsExpired() {

        Instant now = Instant.now();
        Instant expiredTime = now.minus(JobUploadStatus.EXPIRATION_MINUTES, ChronoUnit.MINUTES);

        when(clock.instant()).thenReturn(expiredTime, now);

        jobUploadStatus.changeLastUpdatedTime();
        boolean result = jobUploadStatus.isExpired();
        assertTrue(result);
    }

    @Test
    public void testRemoveBadSession() throws Exception {
        Path jarPath = Files.createTempFile("runjob", ".jar");
        assertTrue(Files.exists(jarPath));
        when(jobMetaDataParameterObject.getJarPath()).thenReturn(jarPath);
        when(jobMetaDataParameterObject.isJarOnClient()).thenReturn(true);

        jobUploadStatus.removeBadSession();

        assertFalse(Files.exists(jarPath));
    }

    @Test
    public void testInvalidCurrentPart() {
        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject();
        jobMultiPartParameterObject.setSessionId(null);
        jobMultiPartParameterObject.setCurrentPartNumber(0);
        jobMultiPartParameterObject.setTotalPartNumber(0);
        jobMultiPartParameterObject.setPartData(null);
        jobMultiPartParameterObject.setPartSize(0);

        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testInvalidTotalPart() {
        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject();
        jobMultiPartParameterObject.setSessionId(null);
        jobMultiPartParameterObject.setCurrentPartNumber(1);
        jobMultiPartParameterObject.setTotalPartNumber(0);
        jobMultiPartParameterObject.setPartData(null);
        jobMultiPartParameterObject.setPartSize(0);

        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testInvalidOrder() {
        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject();
        jobMultiPartParameterObject.setSessionId(null);
        jobMultiPartParameterObject.setCurrentPartNumber(2);
        jobMultiPartParameterObject.setTotalPartNumber(1);
        jobMultiPartParameterObject.setPartData(null);
        jobMultiPartParameterObject.setPartSize(0);

        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testNullPartData() {
        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject();
        jobMultiPartParameterObject.setSessionId(null);
        jobMultiPartParameterObject.setCurrentPartNumber(1);
        jobMultiPartParameterObject.setTotalPartNumber(1);
        jobMultiPartParameterObject.setPartData(null);
        jobMultiPartParameterObject.setPartSize(0);

        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testEmptyPartData() {
        byte[] partData = new byte[]{};

        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject();
        jobMultiPartParameterObject.setSessionId(null);
        jobMultiPartParameterObject.setCurrentPartNumber(1);
        jobMultiPartParameterObject.setTotalPartNumber(1);
        jobMultiPartParameterObject.setPartData(partData);
        jobMultiPartParameterObject.setPartSize(0);

        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testZeroPartSize() {
        byte[] partData = new byte[]{1};

        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject();
        jobMultiPartParameterObject.setSessionId(null);
        jobMultiPartParameterObject.setCurrentPartNumber(1);
        jobMultiPartParameterObject.setTotalPartNumber(1);
        jobMultiPartParameterObject.setPartData(partData);
        jobMultiPartParameterObject.setPartSize(0);

        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testInvalidPartSize() {
        byte[] partData = new byte[]{1};

        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject();
        jobMultiPartParameterObject.setSessionId(null);
        jobMultiPartParameterObject.setCurrentPartNumber(1);
        jobMultiPartParameterObject.setTotalPartNumber(1);
        jobMultiPartParameterObject.setPartData(partData);
        jobMultiPartParameterObject.setPartSize(2);

        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testInvalidSHA256() {
        byte[] partData = new byte[]{1};

        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject();
        jobMultiPartParameterObject.setSessionId(UUID.randomUUID());
        jobMultiPartParameterObject.setCurrentPartNumber(1);
        jobMultiPartParameterObject.setTotalPartNumber(1);
        jobMultiPartParameterObject.setPartData(partData);
        jobMultiPartParameterObject.setPartSize(partData.length);
        jobMultiPartParameterObject.setSha256Hex(null);

        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testChangeLastUpdatedTime() {
        Instant fixedInstant = Instant.ofEpochMilli(1000);
        when(clock.instant()).thenReturn(fixedInstant);

        jobUploadStatus.changeLastUpdatedTime();
        assertEquals(fixedInstant, jobUploadStatus.lastUpdatedTime);
    }

    @Test
    public void testGetJarPath() throws IOException {
        Path jarPath = null;
        try {
            Path path = Paths.get("target");
            Files.createDirectories(path);
            String uploadPath = path.toAbsolutePath().toString();
            when(jobMetaDataParameterObject.getUploadDirectoryPath()).thenReturn(uploadPath);
            jarPath = jobUploadStatus.createJarPath();
            assertTrue(Files.exists(jarPath));
        } finally {
            if (jarPath != null) {
                Files.delete(jarPath);
            }
        }
    }
}
