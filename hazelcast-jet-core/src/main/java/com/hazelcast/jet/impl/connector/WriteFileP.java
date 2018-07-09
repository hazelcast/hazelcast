/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedFunction;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static com.hazelcast.jet.core.processor.SinkProcessors.writeBufferedP;

/**
 * See {@link SinkProcessors#writeFileP(String, DistributedFunction, Charset, boolean)}.
 * <p>
 * Since the work of this sink is file IO-intensive, {@link
 * com.hazelcast.jet.core.Vertex#localParallelism(int) local parallelism} of
 * the vertex should be set according to the performance characteristics of
 * the underlying storage system. Most typically, local parallelism of 1 will
 * already reach the maximum available performance.
 */
public final class WriteFileP {

    private WriteFileP() { }

    /**
     * Use {@link SinkProcessors#writeFileP(String, DistributedFunction, Charset, boolean)}
     */
    public static <T> ProcessorMetaSupplier metaSupplier(
            @Nonnull String directoryName,
            @Nonnull DistributedFunction<T, String> toStringFn,
            @Nonnull String charset,
            boolean append) {

        return ProcessorMetaSupplier.preferLocalParallelismOne(writeBufferedP(
                ctx -> createBufferedWriter(Paths.get(directoryName), ctx.globalProcessorIndex(),
                        charset, append),
                (fileWriter, item) -> {
                    fileWriter.write(toStringFn.apply((T) item));
                    fileWriter.newLine();
                },
                BufferedWriter::flush,
                BufferedWriter::close
        ));
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
            justification = "mkdirs() returns false if the directory already existed, which is good. "
                    + "We don't care even if it didn't exist and we failed to create it, "
                    + "because we'll fail later when trying to create the file.")
    private static BufferedWriter createBufferedWriter(
            Path directory, int globalIndex, String charset, boolean append) throws IOException {
        directory.toFile().mkdirs();

        Path file = directory.resolve(String.valueOf(globalIndex));

        return Files.newBufferedWriter(file,
                Charset.forName(charset), StandardOpenOption.CREATE,
                append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING);
    }

}
