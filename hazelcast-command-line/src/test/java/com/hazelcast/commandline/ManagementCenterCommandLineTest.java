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

import static com.hazelcast.commandline.AbstractCommandLine.LOGGING_PROPERTIES_FINEST_LEVEL;
import static com.hazelcast.commandline.AbstractCommandLine.LOGGING_PROPERTIES_FINE_LEVEL;
import static com.hazelcast.commandline.AbstractCommandLine.WORKING_DIRECTORY;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class ManagementCenterCommandLineTest {
    private ProcessExecutor processExecutor;
    private ManagementCenterCommandLine mcCommandLine;
    private PrintStream out;

    @Before
    public void setUp()
            throws IOException {
        processExecutor = mock(ProcessExecutor.class);
        Process process = mock(Process.class);
        out = mock(PrintStream.class);

        when(process.getInputStream()).thenReturn(mock(InputStream.class));

        mcCommandLine = new ManagementCenterCommandLine(out, mock(PrintStream.class), processExecutor);
    }

    @Test
    public void test_start()
            throws IOException, InterruptedException {
        //when
        mcCommandLine.start(null, null, null, false, false);
        //then
        verify(processExecutor, times(1)).buildAndStart(anyList());
    }

    @Test
    public void test_start_withContextPath()
            throws Exception {
        // given
        String contextPath = "test-path";
        // when
        mcCommandLine.start(contextPath, null, null, false, false);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems("-Dhazelcast.mc.contextPath=" + contextPath)));
    }

    @Test
    public void test_start_withPort()
            throws Exception {
        // given
        String port = "9999";
        // when
        mcCommandLine.start(null, port, null, false, false);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems("-Dhazelcast.mc.http.port=" + port)));
    }

    @Test
    public void test_start_withJavaOpts()
            throws Exception {
        // given
        List<String> javaOpts = new ArrayList<>();
        javaOpts.add("opt1");
        javaOpts.add("opt2");
        // when
        mcCommandLine.start(null, null, javaOpts, false, false);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems(javaOpts.toArray(new String[0]))));
    }

    @Test
    public void test_start_withVerbose()
            throws Exception {
        // given
        boolean verbose = true;
        // when
        mcCommandLine.start(null, null, null, verbose, false);
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
        mcCommandLine.start(null, null, null, false, finestVerbose);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems(
                "-Djava.util.logging.config.file=" + WORKING_DIRECTORY + LOGGING_PROPERTIES_FINEST_LEVEL)));
    }

}
