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

import com.hazelcast.config.Config;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.StringUtil.LINE_SEPARATOR;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DiagnosticsStdoutTest extends HazelcastTestSupport {

    private PrintStream stdout;
    private ByteArrayOutputStream fakeStdout;

    @After
    public void teardown() {
        System.setOut(stdout);
    }

    @Test
    public void testStdoutContent() {
        setup();
        assertTrueEventually(() -> {
            String content = stdoutToString(fakeStdout);
            assertNotNull(content);

            assertContains(content, "SystemProperties[");
            assertContains(content, "BuildInfo[");
            assertContains(content, "ConfigProperties[");
            assertContains(content, "Metric[");

        });
    }

    private void setup() {
        stdout = System.out;
        fakeStdout = new ByteArrayOutputStream();
        System.setOut(new PrintStream(fakeStdout));

        Config config = new Config()
                .setProperty(Diagnostics.ENABLED.getName(), "true")
                .setProperty(Diagnostics.OUTPUT_TYPE.getName(), DiagnosticsOutputType.STDOUT.name())
                .setProperty(MetricsPlugin.PERIOD_SECONDS.getName(), "1");

        createHazelcastInstance(config);
    }

    private static String stdoutToString(ByteArrayOutputStream out) {
        byte[] content = out.toByteArray();
        BufferedReader br = null;
        InputStream is;
        try {
            is = new ByteArrayInputStream(content);
            br = new BufferedReader(new InputStreamReader(is));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(LINE_SEPARATOR);
                line = br.readLine();
            }
            return sb.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            closeResource(br);
        }
    }
}
