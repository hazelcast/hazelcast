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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import java.util.List;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class LoggerSinkTest extends JetTestSupport {

    @Mock AppenderSkeleton appender;
    @Captor ArgumentCaptor<LoggingEvent> logCaptor;

    @Test
    public void loggerSink() {
        // Given
        JetInstance jet = createJetMember();
        String srcName = randomName();
        Logger.getRootLogger().addAppender(appender);
        jet.getList(srcName).add(0);
        Pipeline p = Pipeline.create();

        // When
        p.drawFrom(Sources.<Integer>list(srcName))
         .map(i -> i + "-shouldBeSeenOnTheSystemOutput")
         .drainTo(Sinks.logger());
        jet.newJob(p).join();
        verify(appender, atMost(1000)).doAppend(logCaptor.capture());

        // Then
        List<LoggingEvent> allValues = logCaptor.getAllValues();
        boolean match = allValues
                .stream()
                .map(LoggingEvent::getRenderedMessage)
                .anyMatch(message -> message.contains("0-shouldBeSeenOnTheSystemOutput"));
        Assert.assertTrue(match);
    }
}
