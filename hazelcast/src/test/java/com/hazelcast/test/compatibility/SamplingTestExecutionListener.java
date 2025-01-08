/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.compatibility;

import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.TestEnvironment;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.test.compatibility.SamplingConf.FILE_NAME;
import static com.hazelcast.test.compatibility.SamplingConf.INDEX_FILE_SUFFIX;
import static com.hazelcast.test.compatibility.SamplingConf.SAMPLES_FILE_SUFFIX;
import static com.hazelcast.test.compatibility.SamplingConf.SCHEMA_FILE_SUFFIX;
import static com.hazelcast.test.compatibility.SamplingSerializationService.SERIALIZED_SAMPLES_PER_CLASS_NAME;

/**
 * When tests run is done, dump serialized object samples to output file
 */
public class SamplingTestExecutionListener implements TestExecutionListener {

    private static final ILogger LOGGER = Logger.getLogger(SamplingTestExecutionListener.class);

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        if (isNullOrEmpty(TestEnvironment.getSerializedClassNamesPath())) {
            return;
        }
        LOGGER.info("Sampling is done, serialized classes count: " + SERIALIZED_SAMPLES_PER_CLASS_NAME.keySet().size());
        try (
                var serializedSamplesOutput = new FileOutputStream(FILE_NAME + SAMPLES_FILE_SUFFIX);
                var indexOutput = new FileWriter(FILE_NAME + INDEX_FILE_SUFFIX);
                var serializedSchemaOutput = new FileOutputStream(FILE_NAME + SCHEMA_FILE_SUFFIX)
        ) {
            FileChannel samplesOutputChannel = serializedSamplesOutput.getChannel();
            // index file line format: className,startOfSample1,lengthOfSample1,startOfSample2,lengthOfSample2,...

            for (Map.Entry<String, List<byte[]>> entry : SERIALIZED_SAMPLES_PER_CLASS_NAME.entrySet()) {
                if (entry.getValue().isEmpty()) {
                    continue;
                }
                List<byte[]> samples = entry.getValue();
                indexOutput.write(entry.getKey());
                for (byte[] sample : samples) {
                    indexOutput.write("," + samplesOutputChannel.position() + "," + sample.length);
                    serializedSamplesOutput.write(sample);
                }
                indexOutput.write(System.lineSeparator());
            }

            writeSchemas(serializedSchemaOutput);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeSchemas(FileOutputStream fileOut) {
        Collection<Schema> schemas = SamplingSerializationService.SAMPLED_CLASSES_SCHEMAS.values();
        try (var out = new ObjectOutputStream(fileOut)) {
            Schema.writeSchemas(out, schemas);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
