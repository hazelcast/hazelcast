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

package com.hazelcast.test.compatibility;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.TestEnvironment;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.compatibility.SamplingSerializationService.SERIALIZED_SAMPLES_PER_CLASS_NAME;
import static com.hazelcast.internal.util.StringUtil.LINE_SEPARATOR;

/**
 * When tests run is done, dump serialized object samples to output file
 */
public class SamplingRunListener extends RunListener {

    public static final String FILE_NAME = TestEnvironment.getSerializedClassNamesPath() + randomString();
    public static final String SAMPLES_FILE_SUFFIX = ".samples";
    public static final String INDEX_FILE_SUFFIX = ".index";

    private static final ILogger LOGGER = Logger.getLogger(SamplingRunListener.class);

    @Override
    public void testRunFinished(Result result)
            throws Exception {
        FileOutputStream serializedSamplesOutput = null;
        FileWriter indexOutput = null;
        try {
            serializedSamplesOutput = new FileOutputStream(FILE_NAME + SAMPLES_FILE_SUFFIX);
            FileChannel samplesOutputChannel = serializedSamplesOutput.getChannel();
            // index file line format: className,startOfSample1,lengthOfSample1,startOfSample2,lengthOfSample2,...
            indexOutput = new FileWriter(FILE_NAME + INDEX_FILE_SUFFIX);

            for (Map.Entry<String, List<byte[]>> entry : SERIALIZED_SAMPLES_PER_CLASS_NAME.entrySet()) {
                if (entry.getValue().isEmpty()) {
                    continue;
                }
                List<byte[]> samples = entry.getValue();
                indexOutput.write(entry.getKey());
                for (int i = 0; i < samples.size(); i++) {
                    byte[] sample = samples.get(i);
                    indexOutput.write("," + samplesOutputChannel.position() + "," + sample.length);
                    serializedSamplesOutput.write(sample);
                }
                indexOutput.write(LINE_SEPARATOR);
            }
        } catch (FileNotFoundException e) {
            LOGGER.severe(e);
        } catch (IOException e) {
            LOGGER.severe(e);
        } finally {
            closeResource(indexOutput);
            closeResource(serializedSamplesOutput);
        }
    }
}
