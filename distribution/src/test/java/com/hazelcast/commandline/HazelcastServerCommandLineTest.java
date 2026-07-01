/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.commandline;

import com.hazelcast.function.ThrowingRunnable;
import com.hazelcast.test.SerialTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.util.RestoreSystemProperties;
import org.junitpioneer.jupiter.SetSystemProperty;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.commandline.HazelcastServerCommandLine.createPrintWriter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@ParallelJVMTest
@SerialTest
class HazelcastServerCommandLineTest {
    private HazelcastServerCommandLine hazelcastServerCommandLine;

    @Mock
    private ThrowingRunnable start;

    @BeforeEach
    void setUp() {
        hazelcastServerCommandLine = new HazelcastServerCommandLine(start);
    }

    @Test
    @RestoreSystemProperties
    void test_start() {
        // when
        hazelcastServerCommandLine.start(null, null, null);
        // then
        verify(start, times(1)).run();
    }

    @Test
    @RestoreSystemProperties
    void test_start_withConfigFile() {
        // given
        String configFile = "path/to/test-hazelcast.xml";
        // when
        hazelcastServerCommandLine.start(configFile, null, null);

        assertThat(System.getProperties()).containsEntry("hazelcast.config", configFile);
    }

    @Test
    @RestoreSystemProperties
    void test_start_withPort() {
        // given
        String port = "9999";
        // when
        hazelcastServerCommandLine.start(null, port, null);

        assertThat(System.getProperties()).containsEntry("hz.network.port.port", port);
    }

    @Test
    @RestoreSystemProperties
    void test_start_withInterface() {
        // given
        String hzInterface = "1.1.1.1";
        // when
        hazelcastServerCommandLine.start(null, null, hzInterface);
        // then
        assertThat(System.getProperties()).containsEntry("hz.network.interfaces.interfaces.interface1", hzInterface);
    }

    @Test
    @SetSystemProperty(key = "hz.network.port.port", value = "-1")
    @RestoreSystemProperties
    void test_exception_stack_traces_are_logged() throws Exception {
        try (ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
             PrintStream errorPrintStream = new PrintStream(outputStreamCaptor)) {
            CommandLine cmd = new CommandLine(new HazelcastServerCommandLine())
                    .setOut(createPrintWriter(System.out)).setErr(createPrintWriter(errorPrintStream))
                    .setTrimQuotes(true).setExecutionExceptionHandler(new ExceptionHandler());

            cmd.execute("start");

            String string = outputStreamCaptor.toString(StandardCharsets.UTF_8);
            assertThat(string).contains("IllegalArgumentException: Port out of range: -1. Allowed range [0,65535]");
        }
    }
}
