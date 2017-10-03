/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl.sources;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.stream.AbstractStreamTest;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.jet.stream.IStreamList;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.of;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SourcesTest extends AbstractStreamTest {

    @Test
    public void testFile() throws IOException {
        File dir = createTempFile();
        ProcessorSupplier processorSupplier = SourceProcessors.readFiles(dir.getAbsolutePath(), UTF_8, "*");
        IStreamList<String> sink = DistributedStream
                .<String>fromSource(getInstance(), ProcessorMetaSupplier.of(processorSupplier))
                .flatMap(line -> Arrays.stream(line.split(" ")))
                .collect(DistributedCollectors.toIList(dir.getName()));

        assertEquals(10, sink.size());
        deleteTempFile(dir);
    }

    @Test
    public void testCustomSource() {
        ProcessorMetaSupplier metaSupplier = of(new DummySupplier());
        IStreamList<String> sink = DistributedStream
                .<String>fromSource(getInstance(), metaSupplier)
                .flatMap(line -> Arrays.stream(line.split(" ")))
                .collect(DistributedCollectors.toIList(randomString()));

        assertEquals(10, sink.size());
    }

    private static File createTempFile() throws IOException {
        File directory = Files.createTempDirectory("read-file-p").toFile();
        directory.deleteOnExit();
        File file = new File(directory, randomString());
        file.deleteOnExit();
        PrintWriter printWriter = new PrintWriter(new FileOutputStream(file, true));
        printWriter.write("Hello World!\n");
        printWriter.write("How are you?\n");
        printWriter.close();
        return directory;
    }

    private static void deleteTempFile(File dir) {
        File[] files = dir.listFiles();
        for (File file : files) {
            assertTrue(file.delete());
        }
        assertTrue(dir.delete());
    }

    private static class DummySupplier implements ProcessorSupplier {
        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return IntStream.range(0, count).mapToObj(i -> {
                if (i == 0) {
                    return new DummySource();
                }
                return Processors.noop().get();
            }).collect(Collectors.toList());
        }
    }

    private static class DummySource extends AbstractProcessor {

        @Override
        public boolean complete() {
            emit("Hello World!");
            emit("How are you?");
            return true;
        }
    }
}
