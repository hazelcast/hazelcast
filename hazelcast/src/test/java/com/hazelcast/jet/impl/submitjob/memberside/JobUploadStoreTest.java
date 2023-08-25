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

import com.hazelcast.internal.util.Sha256Util;
import com.hazelcast.mock.MockUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JobUploadStoreTest {
    @Spy
    ConcurrentHashMap<UUID, JobUploadStatus> jobMap;
    @InjectMocks
    JobUploadStore jobUploadStore;

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
    public void testCleanExpiredUploads() {
        UUID sessionID = UUID.randomUUID();

        Instant now = Instant.now();
        Instant expiredTime = now.minus(JobUploadStatus.EXPIRATION_MINUTES, ChronoUnit.MINUTES);

        JobMetaDataParameterObject jobMetaDataParameterObject = new JobMetaDataParameterObject();
        //Create a mock clock
        Clock mock = mock(Clock.class);
        when(mock.instant()).thenReturn(expiredTime, now);
        JobUploadStatus jobUploadStatus = new JobUploadStatus(jobMetaDataParameterObject, mock);


        jobMap.put(sessionID, jobUploadStatus);
        jobUploadStore.cleanExpiredUploads();

        assertThat(jobMap).isEmpty();
    }

    @Test
    public void testRemove() throws Exception {
        UUID sessionID = UUID.randomUUID();
        jobUploadStore.removeBadSession(sessionID);
    }

    @Test
    public void testProcessJarMetaData() throws IOException {
        UUID sessionID = UUID.randomUUID();
        JobMetaDataParameterObject parameterObject = new JobMetaDataParameterObject();
        parameterObject.setSessionId(sessionID);
        jobUploadStore.processJobMetaData(parameterObject);

        assertEquals(1, jobMap.size());
    }

    @Test
    public void testProcessJarData() throws Exception {
        UUID sessionID = UUID.randomUUID();
        JobMetaDataParameterObject parameterObject = new JobMetaDataParameterObject();
        // Set
        parameterObject.setSessionId(sessionID);

        // Set
        byte[] jarData = {(byte) 0};
        String sha256Hex = getSha256Hex(jarData);
        parameterObject.setSha256Hex(sha256Hex);

        // Send meta data
        jobUploadStore.processJobMetaData(parameterObject);

        // Send part 1
        JobMultiPartParameterObject parameterObject1 = new JobMultiPartParameterObject();
        parameterObject1.setSessionId(sessionID);
        parameterObject1.setCurrentPartNumber(1);
        parameterObject1.setTotalPartNumber(2);
        parameterObject1.setPartData(jarData);
        parameterObject1.setPartSize(jarData.length);
        parameterObject1.setSha256Hex(Sha256Util.calculateSha256Hex(jarData, jarData.length));

        JobMetaDataParameterObject result = jobUploadStore.processJobMultipart(parameterObject1);
        assertNull(result);

        // Send part 2
        JobMultiPartParameterObject parameterObject2 = new JobMultiPartParameterObject();
        parameterObject2.setSessionId(sessionID);
        parameterObject2.setCurrentPartNumber(2);
        parameterObject2.setTotalPartNumber(2);
        parameterObject2.setPartData(jarData);
        parameterObject2.setPartSize(jarData.length);
        parameterObject2.setSha256Hex(Sha256Util.calculateSha256Hex(jarData, jarData.length));

        result = jobUploadStore.processJobMultipart(parameterObject2);

        // Assert result
        assertNotNull(result);
        assertTrue(Files.exists(parameterObject.getJarPath()));

        jobUploadStore.removeBadSession(sessionID);
    }

    private String getSha256Hex(byte[] jarData) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        // Two times because we are sending two times
        messageDigest.update(jarData);
        messageDigest.update(jarData);
        byte[] digest = messageDigest.digest();
        BigInteger bigInteger = new BigInteger(1, digest);
        return bigInteger.toString(16);
    }
}
