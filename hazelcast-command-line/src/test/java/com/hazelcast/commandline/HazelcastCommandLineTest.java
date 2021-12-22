/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.commandline;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.commandline.AbstractCommandLine.CLASSPATH_SEPARATOR;
import static com.hazelcast.commandline.AbstractCommandLine.LOGGING_PROPERTIES_FINEST_LEVEL;
import static com.hazelcast.commandline.AbstractCommandLine.LOGGING_PROPERTIES_FINE_LEVEL;
import static com.hazelcast.commandline.AbstractCommandLine.WORKING_DIRECTORY;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class HazelcastCommandLineTest {
    private ProcessExecutor processExecutor;
    private HazelcastCommandLine hazelcastCommandLine;

    @Before
    public void setUp() {
        processExecutor = mock(ProcessExecutor.class);
        Process process = mock(Process.class);

        when(process.getInputStream()).thenReturn(mock(InputStream.class));

        hazelcastCommandLine = new HazelcastCommandLine(mock(PrintStream.class), mock(PrintStream.class), processExecutor);
    }

    @Test
    public void test_start()
            throws IOException, InterruptedException {
        //when
        hazelcastCommandLine.start(null, null, null, null, null, false, false);
        //then
        verify(processExecutor, times(1)).buildAndStart(anyList());
    }

    @Test
    public void test_start_withConfigFile()
            throws Exception {
        // given
        String configFile = "path/to/test-hazelcast.xml";
        // when
        hazelcastCommandLine.start(configFile, null, null, null, null, false, false);
        // then
        verify(processExecutor)
                .buildAndStart((List<String>) argThat(Matchers.hasItems("-Dhazelcast.config=" + configFile)));
    }

    @Test
    public void test_start_withPort()
            throws Exception {
        // given
        String port = "9999";
        // when
        hazelcastCommandLine.start(null, port, null, null, null, false, false);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems("-Dnetwork.port=" + port)));
    }

    @Test
    public void test_start_withInterface()
            throws Exception {
        // given
        String hzInterface = "1.1.1.1";
        // when
        hazelcastCommandLine.start(null, null, hzInterface, null, null, false, false);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems("-Dnetwork.interface=" + hzInterface)));
    }

    @Test
    public void test_start_withAdditionalClasspath()
            throws Exception {
        // given
        String[] additonalClasspath = {"class1", "class2"};
        // when
        hazelcastCommandLine.start(null, null, null, additonalClasspath, null, false, false);
        // then
        StringBuilder out = new StringBuilder();
        for (String classpath : additonalClasspath) {
            out.append(CLASSPATH_SEPARATOR).append(classpath);
        }
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems(Matchers.containsString(out.toString()))));
    }

    @Test
    public void test_start_withJavaOpts()
            throws Exception {
        // given
        List<String> javaOpts = new ArrayList<>();
        javaOpts.add("opt1");
        javaOpts.add("opt2");
        // when
        hazelcastCommandLine.start(null, null, null, null, javaOpts, false, false);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems(javaOpts.toArray(new String[0]))));
    }

    @Test
    public void test_start_withVerbose()
            throws Exception {
        // given
        boolean verbose = true;
        // when
        hazelcastCommandLine.start(null, null, null, null, null, verbose, false);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems(
                "-Djava.util.logging.config.file=" + WORKING_DIRECTORY + LOGGING_PROPERTIES_FINE_LEVEL)));
    }

    @Test
    public void test_start_withVVerbose()
            throws Exception {
        // given
        boolean finestVerbose = true;
        // when
        hazelcastCommandLine.start(null, null, null, null, null, false, finestVerbose);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems(
                "-Djava.util.logging.config.file=" + WORKING_DIRECTORY + LOGGING_PROPERTIES_FINEST_LEVEL)));
    }

    @Test
    public void test_start_ModularJavaOptions()
            throws Exception {
        // given
        System.setProperty("java.specification.version", "9");
        // when
        hazelcastCommandLine.start(null, null, null, null, null, false, false);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(
                Matchers.hasItems("--add-modules", "java.se", "--add-exports", "java.base/jdk.internal.ref=ALL-UNNAMED",
                        "--add-opens", "java.base/java.lang=ALL-UNNAMED", "--add-opens", "java.base/java.nio=ALL-UNNAMED",
                        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens",
                        "java.management/sun.management=ALL-UNNAMED", "--add-opens",
                        "jdk.management/com.sun.management.internal=ALL-UNNAMED")));
    }
}
