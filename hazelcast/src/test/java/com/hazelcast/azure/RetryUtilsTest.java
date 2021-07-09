/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.azure;

import com.hazelcast.core.HazelcastException;
import org.junit.Test;

import java.util.concurrent.Callable;

import static com.hazelcast.azure.RetryUtils.BACKOFF_MULTIPLIER;
import static com.hazelcast.azure.RetryUtils.INITIAL_BACKOFF_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RetryUtilsTest {
    private static final Integer RETRIES = 1;
    private static final String RESULT = "result string";

    private Callable<String> callable = mock(Callable.class);

    @Test
    public void retryNoRetries()
            throws Exception {
        // given
        given(callable.call()).willReturn(RESULT);

        // when
        String result = RetryUtils.retry(callable, RETRIES);

        // then
        assertEquals(RESULT, result);
        verify(callable).call();
    }

    @Test
    public void retryRetriesSuccessful()
            throws Exception {
        // given
        given(callable.call()).willThrow(new RuntimeException()).willReturn(RESULT);

        // when
        String result = RetryUtils.retry(callable, RETRIES);

        // then
        assertEquals(RESULT, result);
        verify(callable, times(2)).call();
    }

    @Test(expected = RuntimeException.class)
    public void retryRetriesFailed()
            throws Exception {
        // given
        given(callable.call()).willThrow(new RuntimeException()).willThrow(new RuntimeException()).willReturn(RESULT);

        // when
        RetryUtils.retry(callable, RETRIES);

        // then
        // throws exception
    }

    @Test(expected = HazelcastException.class)
    public void retryRetriesFailedUncheckedException()
            throws Exception {
        // given
        given(callable.call()).willThrow(new Exception()).willThrow(new Exception()).willReturn(RESULT);

        // when
        RetryUtils.retry(callable, RETRIES);

        // then
        // throws exception
    }

    @Test
    public void retryRetriesWaitExponentialBackoff()
            throws Exception {
        // given
        double twoBackoffIntervalsMs = INITIAL_BACKOFF_MS + (BACKOFF_MULTIPLIER * INITIAL_BACKOFF_MS);
        given(callable.call()).willThrow(new RuntimeException()).willThrow(new RuntimeException()).willReturn(RESULT);

        // when
        long startTimeMs = System.currentTimeMillis();
        RetryUtils.retry(callable, 5);
        long endTimeMs = System.currentTimeMillis();

        // then
        assertTrue(twoBackoffIntervalsMs < (endTimeMs - startTimeMs));
    }

}
