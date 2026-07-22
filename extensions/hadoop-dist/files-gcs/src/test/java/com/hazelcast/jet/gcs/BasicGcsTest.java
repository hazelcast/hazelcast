/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.gcs;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.test.SerialTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

@SerialTest
public class BasicGcsTest extends JetTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @AfterEach
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    void invalidKeyTest() {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        String path = System.getProperty("gcs.test.path");
        String keyValue = System.getProperty("google.key.file");
        String invalidKeyValue = System.getProperty("google.key.file.invalid");
        assumeThat(keyValue).isNotNull();
        assumeThat(invalidKeyValue).isNotNull();

        File invalidFile = new File(invalidKeyValue);

        FileSourceBuilder<String> source = FileSources.files(path)
                                                      .glob("file.txt")
                                                      .format(FileFormat.text());
        source.option("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
        source.option("fs.gs.auth.service.account.json.keyfile", invalidFile.getAbsolutePath());

        source.option("fs.gs.impl.disable.cache", "true");
        Pipeline p = Pipeline.create();

        p.readFrom(source.build())
         .writeTo(Sinks.logger());

        JetService jet = instance.getJet();

        assertThatThrownBy(() -> jet.newJob(p).join())
            .hasCauseInstanceOf(JetException.class)
            .rootCause()
            .hasMessageContaining("Invalid JWT Signature.");
    }
}
