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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.processor.DiagnosticProcessors.writeLoggerP;
import static com.hazelcast.jet.core.test.TestSupport.supplierFrom;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteLoggerPTest {

    @Test
    public void test() throws Exception {
        // Given
        Processor p = supplierFrom(writeLoggerP()).get();
        TestInbox inbox = new TestInbox();
        Outbox outbox = mock(Outbox.class);
        ILogger logger = mock(ILogger.class);
        p.init(outbox, new TestProcessorContext().setLogger(logger));

        // When
        inbox.add(1);
        p.process(0, inbox);
        Watermark wm = new Watermark(2);
        p.tryProcessWatermark(wm);

        // Then
        verifyZeroInteractions(outbox);
        verify(logger).info("1");
        verify(logger).fine(wm.toString());
        verifyZeroInteractions(logger);
    }
}
