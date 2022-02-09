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

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Handler for process operations.
 */
class ProcessExecutor {

    void buildAndStart(List<String> commandList, Map<String, String> environment, Redirect redirectOutput,
                       Redirect redirectError, boolean daemon) throws IOException, InterruptedException {
        List<String> effectiveCommandList = new ArrayList<>(commandList);
        if (daemon) {
            effectiveCommandList.add(0, "nohup");
        }
        ProcessBuilder processBuilder = new ProcessBuilder(effectiveCommandList);
        processBuilder.redirectError(redirectError);
        processBuilder.redirectOutput(redirectOutput);
        processBuilder.environment().putAll(environment);
        Process process = processBuilder.start();
        if (!daemon) {
            // Register shutdown hook so when something kills us we destroy the Hazelcast process
            Runtime.getRuntime().addShutdownHook(new Thread(process::destroy));

            process.waitFor();
        }
    }

}

