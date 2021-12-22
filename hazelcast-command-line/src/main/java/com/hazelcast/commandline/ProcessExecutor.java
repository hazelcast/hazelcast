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

import java.io.IOException;
import java.util.List;

/**
 * Handler for process operations.
 */
class ProcessExecutor {

    void buildAndStart(List<String> commandList)
            throws IOException, InterruptedException {
        ProcessBuilder processBuilder = createProcessBuilder(commandList);
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.start().waitFor();
    }

    /**
     * For test purposes.
     */
    ProcessBuilder createProcessBuilder(List<String> commandList) {
        return new ProcessBuilder(commandList);
    }
}

