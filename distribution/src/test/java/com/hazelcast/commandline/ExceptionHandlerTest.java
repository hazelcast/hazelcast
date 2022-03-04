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

import org.junit.Before;
import org.junit.Test;
import picocli.CommandLine;

import java.io.PrintWriter;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExceptionHandlerTest {

    private Exception exception;
    private CommandLine commandLine;
    private CommandLine.ParseResult parseResult;
    private PrintWriter err;
    private CommandLine.Help.ColorScheme colorScheme;

    @Before
    public void setup() {
        exception = mock(Exception.class);
        commandLine = mock(CommandLine.class);
        parseResult = mock(CommandLine.ParseResult.class);
        err = mock(PrintWriter.class);
        colorScheme = mock(CommandLine.Help.ColorScheme.class);
        when(commandLine.getErr()).thenReturn(err);
        when(commandLine.getColorScheme()).thenReturn(colorScheme);
    }

    @Test
    public void test_handleExecutionException() {
        // given
        doReturn("some message").when(exception).getMessage();
        // when
        new ExceptionHandler().handleExecutionException(exception, commandLine, parseResult);
        // then
        verify(err, times(1)).println(colorScheme.errorText(exception.getMessage()));
        verify(commandLine, times(1)).usage(err, colorScheme);
    }

    @Test
    public void test_handleExecutionException_withoutExceptionMessage() {
        // when
        new ExceptionHandler().handleExecutionException(exception, commandLine, parseResult);
        // then
        verify(exception, times(1)).printStackTrace(err);
        verify(commandLine, times(1)).usage(err, colorScheme);
    }
}
