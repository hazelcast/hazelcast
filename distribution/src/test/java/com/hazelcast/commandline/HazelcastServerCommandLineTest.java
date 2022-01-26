/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.commandline.HazelcastServerCommandLine.CLASSPATH_SEPARATOR;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class HazelcastServerCommandLineTest {
    private static final String WORKING_DIRECTORY = "/hazelcast";
    private ProcessExecutor processExecutor;
    private HazelcastServerCommandLine hazelcastServerCommandLine;

    @Before
    public void setUp() {
        processExecutor = mock(ProcessExecutor.class);
        Process process = mock(Process.class);

        Map<String, String> envVariables = new HashMap<>();
        envVariables.put("HAZELCAST_HOME", WORKING_DIRECTORY);

        when(process.getInputStream()).thenReturn(mock(InputStream.class));

        hazelcastServerCommandLine = new HazelcastServerCommandLine(mock(PrintStream.class),
                processExecutor, envVariables::get);
    }

    @Test
    public void test_start()
            throws IOException, InterruptedException {
        //when
        hazelcastServerCommandLine.start(null, null, null, null, null, false, false, false);
        //then
        verify(processExecutor, times(1)).buildAndStart(anyList(), anyMap(), eq(Redirect.INHERIT), eq(Redirect.INHERIT), eq(false));
    }

    @Test
    public void test_start_withConfigFile()
            throws Exception {
        // given
        String configFile = "path/to/test-hazelcast.xml";
        // when
        hazelcastServerCommandLine.start(configFile, null, null, null, null, false, false, false);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems("-Dhazelcast.config=" + configFile)),
                anyMap(), eq(Redirect.INHERIT), eq(Redirect.INHERIT), eq(false));
    }

    @Test
    public void test_start_withPort()
            throws Exception {
        // given
        String port = "9999";
        // when
        hazelcastServerCommandLine.start(null, port, null, null, null, false, false, false);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems("-Dhz.network.port.port=" + port)),
                anyMap(), eq(Redirect.INHERIT), eq(Redirect.INHERIT), eq(false));
    }

    @Test
    public void test_start_withInterface()
            throws Exception {
        // given
        String hzInterface = "1.1.1.1";
        // when
        hazelcastServerCommandLine.start(null, null, hzInterface, null, null, false, false, false);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems("-Dhz.network.interfaces.interfaces.interface1=" + hzInterface)),
                anyMap(), eq(Redirect.INHERIT), eq(Redirect.INHERIT), eq(false));
    }

    @Test
    public void test_start_withAdditionalClasspath()
            throws Exception {
        // given
        String[] additonalClasspath = {"class1", "class2"};
        // when
        hazelcastServerCommandLine.start(null, null, null, additonalClasspath, null, false, false, false);
        // then
        StringBuilder out = new StringBuilder();
        for (String classpath : additonalClasspath) {
            out.append(CLASSPATH_SEPARATOR).append(classpath);
        }
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems(Matchers.containsString(out.toString()))),
                anyMap(), eq(Redirect.INHERIT), eq(Redirect.INHERIT), eq(false));
    }

    @Test
    public void test_start_withJavaOpts()
            throws Exception {
        // given
        List<String> javaOpts = new ArrayList<>();
        javaOpts.add("opt1");
        javaOpts.add("opt2");
        // when
        hazelcastServerCommandLine.start(null, null, null, null, javaOpts, false, false, false);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(Matchers.hasItems(javaOpts.toArray(new String[0]))),
                anyMap(), eq(Redirect.INHERIT), eq(Redirect.INHERIT), eq(false));
    }

    @Test
    public void test_start_withVerbose()
            throws Exception {
        // given
        boolean verbose = true;
        // when
        hazelcastServerCommandLine.start(null, null, null, null, null, false, verbose, false);
        // then
        verify(processExecutor).buildAndStart(anyList(), (Map<String, String>) argThat(Matchers.hasEntry("LOGGING_LEVEL", "DEBUG")),
                eq(Redirect.INHERIT), eq(Redirect.INHERIT), eq(false));
    }

    @Test
    public void test_start_withVVerbose()
            throws Exception {
        // given
        boolean finestVerbose = true;
        // when
        hazelcastServerCommandLine.start(null, null, null, null, null, false, false, finestVerbose);
        // then
        verify(processExecutor).buildAndStart(anyList(), (Map<String, String>) argThat(Matchers.hasEntry("LOGGING_LEVEL", "TRACE")),
                eq(Redirect.INHERIT), eq(Redirect.INHERIT), eq(false));
    }

    @Test
    public void test_start_ModularJavaOptions()
            throws Exception {
        // given
        System.setProperty("java.specification.version", "9");
        // when
        hazelcastServerCommandLine.start(null, null, null, null, null, false, false, false);
        // then
        verify(processExecutor).buildAndStart((List<String>) argThat(
                Matchers.hasItems("--add-modules", "java.se", "--add-exports", "java.base/jdk.internal.ref=ALL-UNNAMED",
                        "--add-opens", "java.base/java.lang=ALL-UNNAMED",
                        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens",
                        "java.management/sun.management=ALL-UNNAMED", "--add-opens",
                        "jdk.management/com.sun.management.internal=ALL-UNNAMED")), anyMap(), eq(Redirect.INHERIT), eq(Redirect.INHERIT), eq(false));
    }

    @Test
    public void test_start_withDaemon()
            throws Exception {
        // when
        hazelcastServerCommandLine.start(null, null, null, null, null, true, false, false);
        // then
        verify(processExecutor).buildAndStart(
                anyList(),
                anyMap(), argThat(redirect -> redirect.file().getName().toLowerCase().contains("nul")),
                argThat(redirect -> redirect.type() == Redirect.Type.WRITE),
                eq(true)
        );
    }

}
