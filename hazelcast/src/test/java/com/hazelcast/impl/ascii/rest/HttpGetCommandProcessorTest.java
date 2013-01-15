/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl.ascii.rest;

import com.hazelcast.impl.ascii.TextCommandService;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class HttpGetCommandProcessorTest {
    private static final String uri_map_prefix = "/hazelcast/rest/maps/";
    private static final String uri_map_correct = uri_map_prefix + "testmap/testkey";

    private static final String uri_queue_prefix = "/hazelcast/rest/queues/";
    private static final String uri_queue_correct = uri_queue_prefix + "testqueue";
    private static final String uri_queue_correct_slash = uri_queue_prefix + "testqueue/";

    private final String TEST_MAP = "testmap";
    private final String TEST_KEY = "testkey";
    private final String TEST_QUEUE = "testqueue";
    private final String TEST_VALUE = "testvalue";

    private static HttpGetCommand expected_result;
    private static final TextCommandService mockTextCommandService = mock(TextCommandService.class);
    private static HttpGetCommandProcessor componentUnderTest;

    @BeforeClass
    public static void initialSetup() {
        componentUnderTest = new HttpGetCommandProcessor(mockTextCommandService);
        expected_result = new HttpGetCommand(uri_map_correct);
        expected_result.setResponse(HttpCommand.CONTENT_TYPE_PLAIN_TEXT, ("testvalue").getBytes());
    }

    @After
    public void resetMocks() {
        reset(mockTextCommandService);
    }

    @Test
    public void testQueryNonExistingMap() {
        when(mockTextCommandService.get(TEST_MAP, TEST_KEY)).thenReturn(null);
        HttpGetCommand command = new HttpGetCommand(uri_map_correct);
        componentUnderTest.handle(command);
        verify(mockTextCommandService).get(TEST_MAP, TEST_KEY);
        assertEquals("Response is empty 204", HttpGetCommand.RES_204, command.response.array());
    }

    @Test
    public void testQueryExistingMap() {
        when(mockTextCommandService.get(TEST_MAP, TEST_KEY)).thenReturn(TEST_VALUE);
        HttpGetCommand command = new HttpGetCommand(uri_map_correct);
        componentUnderTest.handle(command);
        verify(mockTextCommandService).get(TEST_MAP, TEST_KEY);
        assertEquals("Result array not matches expectation", expected_result.response, command.response);
    }

    @Test
    public void testQueryEmptyQueue() {
        when(mockTextCommandService.poll(TEST_QUEUE)).thenReturn(null);
        HttpGetCommand command = new HttpGetCommand(uri_queue_correct);
        componentUnderTest.handle(command);
        verify(mockTextCommandService).poll(TEST_QUEUE);
        assertEquals("Response is empty 204", HttpGetCommand.RES_204, command.response.array());
    }

    @Test
    public void testQueryFullQueue() {
        when(mockTextCommandService.poll(TEST_QUEUE)).thenReturn(TEST_VALUE);
        HttpGetCommand command = new HttpGetCommand(uri_queue_correct);
        componentUnderTest.handle(command);
        verify(mockTextCommandService).poll(TEST_QUEUE);
        assertEquals("Result array not matches expectation", expected_result.response, command.response);
    }

    @Test
    public void testQueryQueueTrailingSlash() {
        when(mockTextCommandService.poll(TEST_QUEUE)).thenReturn(TEST_VALUE);
        HttpGetCommand command = new HttpGetCommand(uri_queue_correct_slash);
        componentUnderTest.handle(command);
        verify(mockTextCommandService).poll(TEST_QUEUE);
        assertEquals("Result array not matches expectation", expected_result.response, command.response);
    }
}
