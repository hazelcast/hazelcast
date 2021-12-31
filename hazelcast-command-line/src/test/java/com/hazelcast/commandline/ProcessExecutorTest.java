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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.contentOf;
import static org.awaitility.Awaitility.await;


public class ProcessExecutorTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void test_buildAndStart() throws IOException, InterruptedException {
        // given
        File outputFile = temporaryFolder.newFile();
        ProcessExecutor processExecutor = new ProcessExecutor();
        // when
        processExecutor.buildAndStart(singletonList("whoami"), ProcessBuilder.Redirect.to(outputFile), false);
        // then
        assertThat(contentOf(outputFile)).contains(System.getProperty("user.name"));
    }

    @Test
    public void test_buildAndStart_daemon() throws IOException, InterruptedException {
        // given
        File outputFile = temporaryFolder.newFile();
        ProcessExecutor processExecutor = new ProcessExecutor();
        // when
        processExecutor.buildAndStart(singletonList("whoami"), ProcessBuilder.Redirect.to(outputFile), true);
        // then
        await().atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(contentOf(outputFile)).contains(System.getProperty("user.name"));
        });
    }
}
