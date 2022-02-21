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

package com.hazelcast.commandline;

import com.hazelcast.jet.function.RunnableEx;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class HazelcastServerCommandLineTest {
    private HazelcastServerCommandLine hazelcastServerCommandLine;
    private RunnableEx start;

    @Before
    public void setUp() {
        start = mock(RunnableEx.class);
        hazelcastServerCommandLine = new HazelcastServerCommandLine(start);
    }

    @Test
    public void test_start()
            throws IOException, InterruptedException {
        //when
        hazelcastServerCommandLine.start(null, null, null);
        //then
        verify(start, times(1)).run();
    }

    @Test
    public void test_start_withConfigFile()
            throws Exception {
        // given
        String configFile = "path/to/test-hazelcast.xml";
        // when
        hazelcastServerCommandLine.start(configFile, null, null);

        assertThat(System.getProperties()).containsEntry("hazelcast.config", configFile);
    }

    @Test
    public void test_start_withPort()
            throws Exception {
        // given
        String port = "9999";
        // when
        hazelcastServerCommandLine.start(null, port, null);

        assertThat(System.getProperties()).containsEntry("hz.network.port.port", port);
    }

    @Test
    public void test_start_withInterface()
            throws Exception {
        // given
        String hzInterface = "1.1.1.1";
        // when
        hazelcastServerCommandLine.start(null, null, hzInterface);
        // then
        assertThat(System.getProperties()).containsEntry("hz.network.interfaces.interfaces.interface1", hzInterface);
    }

}
