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

import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProcessExecutorTest {
    @Test
    public void test_buildAndStart()
            throws IOException, InterruptedException {
        // given
        List<String> commandList = mock(List.class);
        ProcessBuilder processBuilder = mock(ProcessBuilder.class);
        ProcessExecutor processExecutor = spy(ProcessExecutor.class);
        doReturn(processBuilder).when(processExecutor).createProcessBuilder(commandList);
        when(processBuilder.start()).thenReturn(mock(Process.class));
        // when
        processExecutor.buildAndStart(commandList);
        // then
        verify(processBuilder, times(1)).start();
    }
}
