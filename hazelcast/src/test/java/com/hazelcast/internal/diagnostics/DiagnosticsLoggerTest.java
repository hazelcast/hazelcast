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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import static com.hazelcast.instance.impl.TestUtil.getNode;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DiagnosticsLoggerTest extends HazelcastTestSupport {

    @Test
    public void testLoggerContent() {
        Config config = new Config()
                .setProperty(Diagnostics.ENABLED.getName(), "true")
                .setProperty(Diagnostics.OUTPUT_TYPE.getName(), DiagnosticsOutputType.LOGGER.name())
                .setProperty(Diagnostics.INCLUDE_EPOCH_TIME.getName(), "false")
                .setProperty(ClusterProperty.LOGGING_ENABLE_DETAILS.getName(), "false")
                .setProperty(MetricsPlugin.PERIOD_SECONDS.getName(), "1");

        HazelcastInstance instance = createHazelcastInstance(config);

        StringBuilder diagnosticsOutput = new StringBuilder();

        getNode(instance).loggingService.addLogListener(Level.FINE, event -> {
            LogRecord record = event.getLogRecord();
            if (record.getLoggerName().equals("com.hazelcast.diagnostics")) {
                diagnosticsOutput.append(record.getMessage());
            }
        });


        assertTrueEventually(() -> {
            String content = diagnosticsOutput.toString();
            assertNotNull(content);

            assertContains(content, "Metric[");
        });
    }
}
