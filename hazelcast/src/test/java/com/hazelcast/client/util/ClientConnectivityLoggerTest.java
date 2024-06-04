/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.util;

import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.lang.String.format;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@Category(QuickTest.class)
class ClientConnectivityLoggerTest {

    @Mock
    private LoggingService loggingService;
    @Mock
    private ScheduledExecutorService executor;
    @Mock
    private ILogger logger;

    @BeforeEach
    public void setUp() {
        when(loggingService.getLogger(ClientConnectivityLogger.class)).thenReturn(logger);
    }

    @Test
    void assertLogMessageIsCorrect() {
        List<Member> mockMembers = createMockMembers(2);
        HazelcastProperties hzProps = createMockProperties(1);
        ClientConnectivityLogger clientConnectivityLogger = new ClientConnectivityLogger(loggingService, executor, hzProps);

        clientConnectivityLogger.submitLoggingTask(List.of(mockMembers.get(0)), mockMembers);

        String expected = format(System.lineSeparator()
                        + System.lineSeparator()
                        + "Client Connectivity [2] {"
                        + System.lineSeparator()
                        + "\t%s - connected"
                        + System.lineSeparator()
                        + "\t%s - disconnected"
                        + System.lineSeparator()
                        + "}"
                        + System.lineSeparator(),
                mockMembers.get(0),
                mockMembers.get(1)
        );

        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).schedule(runnableCaptor.capture(), anyLong(), any());

        runnableCaptor.getValue().run();
        verify(logger).info(expected);
    }

    @Test
    void assertTaskCancelledIfSubmittedDuringRunningTask() {
        ScheduledFuture mockFuture = mock(ScheduledFuture.class);
        List<Member> mockMembers = createMockMembers(2);
        HazelcastProperties hzProps = createMockProperties(10);
        ClientConnectivityLogger clientConnectivityLogger = new ClientConnectivityLogger(loggingService, executor, hzProps);
        when(executor.schedule((Runnable) any(), anyLong(), any())).thenReturn(mockFuture);
        clientConnectivityLogger.submitLoggingTask(List.of(mockMembers.get(0)), mockMembers);
        //submit another task while the first one is running
        clientConnectivityLogger.submitLoggingTask(List.of(mockMembers.get(0)), mockMembers);
        verify(mockFuture, times(1)).cancel(true);

        verify(executor, times(2)).schedule((Runnable) any(), anyLong(), any());
    }


    private HazelcastProperties createMockProperties(int periodSeconds) {
        Properties properties = new Properties();
        properties.setProperty(ClientProperty.CLIENT_CONNECTIVITY_LOGGING_DELAY_SECONDS.getName(), "1");
        return new HazelcastProperties(properties);
    }

    private List<Member> createMockMembers(int count) {
        List<Member> members = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Address memberAddress = Address.createUnresolvedAddress("127.0.0.1", 5701 + i);
            members.add(new MemberImpl(memberAddress, MemberVersion.UNKNOWN, false, UUID.randomUUID()));
        }
        return members;
    }
}
