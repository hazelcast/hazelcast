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

package com.hazelcast.internal.diagnostics;

/**
 * Defines the output type for Hazelcast diagnostics.
 */
public enum DiagnosticsOutputType {

    /**
     * Outputs the diagnostics to a set of files managed by Hazelcast.
     */
    FILE {
        @Override
        DiagnosticsLog newLog(Diagnostics diagnostics) {
            return new DiagnosticsLogFile(diagnostics);
        }
    },

    /**
     * Outputs the diagnostics to the "standard" output stream as determined by
     * {@link System#out}.
     */
    STDOUT {
        @Override
        DiagnosticsLog newLog(Diagnostics diagnostics) {
            return new DiagnosticsStdout(diagnostics);
        }
    },

    /**
     * Outputs the diagnostics to the Hazelcast logger. You may then use your
     * logging configuration to forward the diagnostics to any output supported
     * by the logging framework. You may also want to use some additional configuration
     * to control how the output format.
     * Using the logging framework introduces a slight overhead in comparison
     * to using other output types but allows for greater flexibility.
     *
     * @see com.hazelcast.spi.properties.ClusterProperty#LOGGING_ENABLE_DETAILS
     * @see Diagnostics#INCLUDE_EPOCH_TIME
     */
    LOGGER {
        @Override
        DiagnosticsLog newLog(Diagnostics diagnostics) {
            return new DiagnosticsLogger(diagnostics);
        }
    };

    abstract DiagnosticsLog newLog(Diagnostics diagnostics);
}
