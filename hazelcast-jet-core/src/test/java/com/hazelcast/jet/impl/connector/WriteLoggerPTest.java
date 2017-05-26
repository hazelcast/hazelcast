/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.processor.DiagnosticProcessors;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WriteLoggerPTest {

    @Test
    public void test() {
        // Given
        Processor p = DiagnosticProcessors.writeLogger().get();
        ArrayDequeInbox inbox = new ArrayDequeInbox();
        Outbox outbox = mock(Outbox.class);
        ILogger logger = mock(ILogger.class);
        p.init(outbox, new ProcCtx(null, logger, null, 0));

        // When
        inbox.add(1);
        p.process(0, inbox);

        // Then
        verifyZeroInteractions(outbox);
        verify(logger).info("1");
        verifyZeroInteractions(logger);
    }
}
