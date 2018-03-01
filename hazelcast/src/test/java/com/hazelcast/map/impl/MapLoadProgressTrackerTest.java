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

package com.hazelcast.map.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, NightlyTest.class})
public class MapLoadProgressTrackerTest {

    private static final String TEST_MAP_NAME = "testMap";

    @Mock
    private LoggingService loggingService;
    @Mock
    private ILogger logger;

    @Before
    public void setUp() {
        initMocks(this);
        given(loggingService.getLogger(MapLoadProgressTracker.class)).willReturn(logger);
    }

    @Test
    public void logCountOfLoadedRecordsSinceLastReport() throws Exception {
        MapLoadProgressTracker tracker = new MapLoadProgressTracker(TEST_MAP_NAME, loggingService);

        tracker.onBatchLoaded(100);
        SECONDS.sleep(10);
        tracker.onBatchLoaded(121);

        tracker.onBatchLoaded(101);
        SECONDS.sleep(10);
        tracker.onBatchLoaded(199);

        ArgumentCaptor<String> logCaptor = forClass(String.class);

        verify(logger, times(2)).info(logCaptor.capture());
        verifyNoMoreInteractions(logger);

        List<String> allCapturedLogLine = logCaptor.getAllValues();
        String firstLoggedLine = allCapturedLogLine.get(0);
        String secondLoggedLine = allCapturedLogLine.get(1);

        assertTrue(firstLoggedLine.contains(TEST_MAP_NAME));
        assertTrue(firstLoggedLine.contains(Integer.toString(221)));
        assertTrue(secondLoggedLine.contains(TEST_MAP_NAME));
        assertTrue(secondLoggedLine.contains(Integer.toString(300)));
    }

    @Test
    public void testFirstBatchLoadsDoNotReport() {
        MapLoadProgressTracker tracker = new MapLoadProgressTracker(TEST_MAP_NAME, loggingService);
        tracker.onBatchLoaded(100);
        tracker.onBatchLoaded(100);

        verify(logger, never()).info(anyString());
    }
}
