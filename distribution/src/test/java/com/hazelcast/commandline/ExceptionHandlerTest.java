/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import picocli.CommandLine;

import java.io.PrintWriter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExceptionHandlerTest {

    @Mock
    private Exception exception;

    @Mock
    private CommandLine commandLine;

    @Mock
    private CommandLine.ParseResult parseResult;

    @Mock
    private PrintWriter errorWriter;

    @Mock
    private CommandLine.Help.ColorScheme colorScheme;

    @BeforeEach
    public void setup() {
        when(commandLine.getErr()).thenReturn(errorWriter);
        when(commandLine.getColorScheme()).thenReturn(colorScheme);
    }


    @Test
    void test_handleExecutionException_withoutExceptionMessage() {
        // when
        new ExceptionHandler().handleExecutionException(exception, commandLine, parseResult);
        // then
        verify(exception, times(1)).printStackTrace(any(PrintWriter.class));
        verify(commandLine, times(1)).usage(errorWriter, colorScheme);
    }
}
