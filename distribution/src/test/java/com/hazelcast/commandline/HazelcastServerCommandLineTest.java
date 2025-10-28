/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.apache.commons.io.output.NullOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.SetSystemProperty;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.commandline.HazelcastServerCommandLine.createPrintWriter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@ParallelJVMTest
class HazelcastServerCommandLineTest {
    private HazelcastServerCommandLine hazelcastServerCommandLine;

    @Mock
    private RunnableEx start;

    @Mock
    private RuntimeException exception;

    @BeforeEach
    void setUp() {
        hazelcastServerCommandLine = new HazelcastServerCommandLine(start);
    }

    @Test
    void test_start() {
        // when
        hazelcastServerCommandLine.start(null, null, null);
        // then
        verify(start, times(1)).run();
    }

    @Test
    void test_start_withConfigFile() {
        // given
        String configFile = "path/to/test-hazelcast.xml";
        // when
        hazelcastServerCommandLine.start(configFile, null, null);

        assertThat(System.getProperties()).containsEntry("hazelcast.config", configFile);
    }

    @Test
    void test_start_withPort() {
        // given
        String port = "9999";
        // when
        hazelcastServerCommandLine.start(null, port, null);

        assertThat(System.getProperties()).containsEntry("hz.network.port.port", port);
    }

    @Test
    void test_start_withInterface() {
        // given
        String hzInterface = "1.1.1.1";
        // when
        hazelcastServerCommandLine.start(null, null, hzInterface);
        // then
        assertThat(System.getProperties()).containsEntry("hz.network.interfaces.interfaces.interface1", hzInterface);
    }

    @Test
    @SetSystemProperty(key = "hazelcast.logging.type", value = "log4j2")
    @SetSystemProperty(key = "log4j2.configurationFile", value = "faulty-log.properties")
    void test_log4j2_exception() throws Exception {
        try (ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
             PrintStream errorPrintStream = new PrintStream(outputStreamCaptor)) {
            CommandLine cmd = new CommandLine(new HazelcastServerCommandLine())
                    .setOut(createPrintWriter(System.out)).setErr(createPrintWriter(errorPrintStream))
                    .setTrimQuotes(true).setExecutionExceptionHandler(new ExceptionHandler());
            cmd.execute("start", "--full-stack-traces");

            String string = outputStreamCaptor.toString(StandardCharsets.UTF_8);
            assertThat(string).contains("org.apache.logging.log4j.core.config.ConfigurationException: "
                    + "No type attribute provided for Layout on Appender STDOUT");
        }
    }

    @Test
    void test_fullstacktrace() throws Exception {
        //given
        doThrow(exception).when(start).run();

        //when
        CommandLine cmd = new CommandLine(hazelcastServerCommandLine)
                .setExecutionExceptionHandler(new ExceptionHandler())
                .setErr(new PrintWriter(NullOutputStream.INSTANCE));
        cmd.execute("start", "--full-stack-traces");

        //then
        verify(exception).printStackTrace(any(PrintWriter.class));
    }
}
