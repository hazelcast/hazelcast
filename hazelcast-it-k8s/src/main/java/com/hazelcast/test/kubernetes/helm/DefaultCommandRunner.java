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
package com.hazelcast.test.kubernetes.helm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

class DefaultCommandRunner implements CommandRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCommandRunner.class);

    @Override
    public void run(String... command) {
        String commandString = String.join(" ", Arrays.asList(command));
        LOGGER.info("Helm: {}", commandString);
        try {
            new ProcessBuilder(command).inheritIO().start().waitFor();
        } catch (InterruptedException | IOException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
