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

import org.junit.Before;
import org.junit.Test;
import picocli.CommandLine;
import picocli.CommandLine.Model.OptionSpec;

import java.util.List;
import java.util.Stack;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JavaOptionsConsumerTest {

    private CommandLine.Model.ArgSpec argSpec;
    private CommandLine.Model.CommandSpec commandSpec;
    private String split;

    @Before
    public void setup() {
        split = ",";
        argSpec = OptionSpec.builder("optionName").type(List.class).splitRegex(split).build();
        commandSpec = mock(CommandLine.Model.CommandSpec.class);
        when(commandSpec.commandLine()).thenReturn(mock(CommandLine.class));
    }

    @Test
    public void test_consumeParameters() {
        // given
        String[] options = {"test-option1", "test-option2"};
        Stack<String> args = new Stack<>();
        args.push(options[0] + split + options[1]);
        // when
        AbstractCommandLine.JavaOptionsConsumer javaOptionsConsumer = new AbstractCommandLine.JavaOptionsConsumer();
        javaOptionsConsumer.consumeParameters(args, argSpec, commandSpec);
        // then
        List<String> list = argSpec.getValue();
        for (int i = 0; i < options.length; i++) {
            assertEquals(options[i], list.get(i));
        }
    }

    @Test(expected = CommandLine.ParameterException.class)
    public void test_consumeParameters_noArgs() {
        // given
        Stack<String> args = new Stack<>();
        // when
        AbstractCommandLine.JavaOptionsConsumer javaOptionsConsumer = new AbstractCommandLine.JavaOptionsConsumer();
        javaOptionsConsumer.consumeParameters(args, argSpec, commandSpec);
        // then
        // Exception expected
    }
}
