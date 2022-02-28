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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.MetricDescriptor;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;

/**
 * This class is used for generating the binary file to be used in non-Java clients
 * to validate metrics binary format compatibility.
 *
 * Run this class once. Then move the created file to resources directory.
 *
 * mv *binary src/test/resources/
 */
final class MetricsCompatibilityFileGenerator {

    private MetricsCompatibilityFileGenerator() {
    }

    public static void main(String[] args) throws IOException {
        OutputStream out = new FileOutputStream(createFileName());
        DataOutputStream outputStream = new DataOutputStream(out);
        byte[] blob = generateBlob();
        outputStream.write(blob);
        outputStream.close();
    }

    private static String createFileName() {
        return "metrics.compatibility.binary";
    }

    private static byte[] generateBlob() {
        DefaultMetricDescriptorSupplier supplier = new DefaultMetricDescriptorSupplier();
        MetricsCompressor compressor = new MetricsCompressor();

        MetricDescriptor metric1 = supplier.get()
                                           .withPrefix("prefix")
                                           .withMetric("deltaMetric1")
                                           .withDiscriminator("ds", "dsName1")
                                           .withUnit(COUNT);
        compressor.addLong(metric1, 42);

        MetricDescriptor metric2 = metric1.copy()
                                          .withMetric("deltaMetric2");
        compressor.addDouble(metric2, -4.2);

        // use a 254 byte string to test long words in the dictionary
        String longPrefix = Stream.generate(() -> "a")
                                  .limit(MetricsDictionary.MAX_WORD_LENGTH - 1)
                                  .collect(Collectors.joining());
        MetricDescriptor metric3 = supplier.get()
                                           .withPrefix(longPrefix)
                                           .withMetric("longPrefixMetric")
                                           .withUnit(BYTES);
        compressor.addLong(metric3, Integer.MAX_VALUE);

        return compressor.getBlobAndReset();
    }
}
