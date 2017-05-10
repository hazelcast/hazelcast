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

package com.hazelcast.jet;

import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.jet.Processors.peekInput;
import static com.hazelcast.jet.Processors.peekOutput;
import static com.hazelcast.jet.function.DistributedFunctions.alwaysTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class Processors_peekTest {

    private ArrayDequeInbox inbox;
    private ArrayDequeOutbox outbox;
    private Processor.Context context;
    private ILogger logger;

    @Parameter
    public DistributedFunction<Object, String> toStringF;

    @Parameter(1)
    public DistributedPredicate<Object> shouldLogF;

    @Parameters(name = "toStringF={0}, shouldLogF={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{null, null},
                new Object[]{
                        (DistributedFunction<Object, String>) o -> "a" + o,
                        (DistributedPredicate<Object>) o -> (int) o <= 1
                }
        );
    }

    @Before
    public void before() {
        inbox = new ArrayDequeInbox();
        outbox = new ArrayDequeOutbox(new int[]{128}, new ProgressTracker());
        logger = mock(ILogger.class);
        context = new ProcCtx(null, logger, null, 0);
    }

    @Test
    public void when_peekInput_SupplierProcessor() {
        // Given
        DistributedSupplier<Processor> passThroughPSupplier = Processors.filter(alwaysTrue());
        Processor wrappedP =
                (toStringF == null ? peekInput(passThroughPSupplier) : peekInput(toStringF, shouldLogF, passThroughPSupplier))
                        .get();
        wrappedP.init(outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekInput_ProcessorSupplier() {
        // Given
        ProcessorSupplier passThroughPSupplier = ProcessorSupplier.of(Processors.filter(alwaysTrue()));
        Processor wrappedP =
                (toStringF == null ? peekInput(passThroughPSupplier) : peekInput(toStringF, shouldLogF, passThroughPSupplier))
                        .get(1).iterator().next();
        wrappedP.init(outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekInput_ProcessorMetaSupplier() {
        // Given
        ProcessorMetaSupplier passThroughPSupplier = ProcessorMetaSupplier.of(Processors.filter(alwaysTrue()));
        Address address = new Address();
        Processor wrappedP =
                (toStringF == null ? peekInput(passThroughPSupplier) : peekInput(toStringF, shouldLogF, passThroughPSupplier))
                    .get(Collections.singletonList(address)).apply(address).get(1).iterator().next();
        wrappedP.init(outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekOutput_SupplierProcessor() {
        // Given
        DistributedSupplier<Processor> passThroughPSupplier = Processors.filter(alwaysTrue());
        Processor wrappedP =
                (toStringF == null ? peekInput(passThroughPSupplier) : peekOutput(toStringF, shouldLogF, passThroughPSupplier))
                        .get();

        wrappedP.init(outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekOutput_ProcessorSupplier() {
        // Given
        ProcessorSupplier passThroughPSupplier = ProcessorSupplier.of(Processors.filter(alwaysTrue()));
        Processor wrappedP =
                (toStringF == null ? peekInput(passThroughPSupplier) : peekOutput(toStringF, shouldLogF, passThroughPSupplier))
                        .get(1).iterator().next();
        wrappedP.init(outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekOutput_ProcessorMetaSupplier() {
        // Given
        ProcessorMetaSupplier passThroughPSupplier = ProcessorMetaSupplier.of(Processors.filter(alwaysTrue()));
        Address address = new Address();
        Processor wrappedP =
                (toStringF == null ? peekInput(passThroughPSupplier) : peekOutput(toStringF, shouldLogF, passThroughPSupplier))
                    .get(Collections.singletonList(address)).apply(address).get(1).iterator().next();
        wrappedP.init(outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    private void assertLogged(Processor wrappedP) {
        // When
        inbox.add(1);
        inbox.add(2);
        wrappedP.process(0, inbox);

        // Then
        verify(logger).info(format(1));
        if (shouldLogF == null) {
            verify(logger).info(format(2));
        }
        verifyZeroInteractions(logger);
    }

    private String format(int s) {
        if (toStringF == null)
            return String.valueOf(s);
        else
            return toStringF.apply(s);
    }
}
