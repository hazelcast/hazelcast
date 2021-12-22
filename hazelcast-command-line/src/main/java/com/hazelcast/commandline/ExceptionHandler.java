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

import com.hazelcast.internal.util.StringUtil;
import picocli.CommandLine;

import java.io.PrintWriter;

/**
 * Exception handler for processing the tool & Hazelcast related errors
 */
class ExceptionHandler
        implements CommandLine.IExecutionExceptionHandler {
    @Override
    public int handleExecutionException(Exception ex, CommandLine commandLine, CommandLine.ParseResult parseResult) {
        PrintWriter err = commandLine.getErr();
        CommandLine.Help.ColorScheme colorScheme = commandLine.getColorScheme();
        if (!StringUtil.isNullOrEmpty(ex.getMessage())) {
            err.println(colorScheme.errorText(ex.getMessage()));
        } else {
            ex.printStackTrace(err);
        }
        commandLine.usage(err, colorScheme);
        return 0;
    }
}
