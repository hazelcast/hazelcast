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

package com.hazelcast.jet.impl.jobupload;

import com.hazelcast.jet.JetException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JobUploadStatusTest {
    @Mock
    ILogger logger;
    @Mock
    Clock clock;
    @Mock
    JobMetaDataParameterObject jobMetaDataParameterObject;
    @InjectMocks
    JobUploadStatus jobUploadStatus;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testIsExpired() throws Exception {

        Instant now = Instant.now();
        Instant expiredTime = now.minus(1, ChronoUnit.MINUTES);

        when(clock.instant()).thenReturn(expiredTime, now);

        jobUploadStatus.changeLastUpdatedTime();
        boolean result = jobUploadStatus.isExpired();
        assertTrue(result);
    }

    @Test
    public void testOnRemove() throws Exception {
        Path jarPath = Files.createTempFile("runjob", ".jar");
        assertTrue(Files.exists(jarPath));
        when(jobMetaDataParameterObject.getJarPath()).thenReturn(jarPath);

        jobUploadStatus.onRemove();

        assertFalse(Files.exists(jarPath));
    }

    @Test
    public void testInvalidCurrentPart() {
        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject(null, 0, 0, null, 0);
        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testInvalidTotalPart() {
        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject(null, 1, 0, null, 0);
        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testInvalidOrder() {
        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject(null, 2, 1, null, 0);
        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testNullPartData() {
        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject(null, 1, 1, null, 0);
        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testEmptyPartData() {
        byte[] partData = new byte[]{};
        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject(null, 1, 1, partData, 0);
        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testZeroPartSize() {
        byte[] partData = new byte[]{1};
        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject(null, 1, 1, partData, 0);
        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testInvalidPartSize() {
        byte[] partData = new byte[]{1};
        JobMultiPartParameterObject jobMultiPartParameterObject = new JobMultiPartParameterObject(null, 1, 1, partData, 2);
        Assert.assertThrows(JetException.class, () -> jobUploadStatus.processJobMultipart(jobMultiPartParameterObject));
    }

    @Test
    public void testChangeLastUpdatedTime() {
        Instant fixedInstant = Instant.ofEpochMilli(1000);
        when(clock.instant()).thenReturn(fixedInstant);

        jobUploadStatus.changeLastUpdatedTime();
        assertEquals(fixedInstant, jobUploadStatus.lastUpdatedTime);
    }
}
