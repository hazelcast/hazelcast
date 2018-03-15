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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class LoggerSinkTest extends PipelineTestSupport {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();

    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    @After
    public void restoreStreams() {
        System.setOut(System.out);
        System.setErr(System.err);
    }

    @Test
    public void loggerSink() {
        // Given
        List<Integer> input = sequence(10);
        addToSrcList(input);

        // When
        p.drawFrom(Sources.<Integer>list(srcName))
         .map(i -> i + "-shouldBeSeenOnTheSystemOutput")
         .drainTo(Sinks.logger());
        execute();

        // Then
        input.stream()
             .map(i -> i + "-shouldBeSeenOnTheSystemOutput")
             .forEach(s -> assertTrue("Output should contains -> " + s, outContent.toString().contains(s)));
    }


}
