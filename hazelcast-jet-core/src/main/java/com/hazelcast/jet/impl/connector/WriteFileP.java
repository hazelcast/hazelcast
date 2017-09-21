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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.core.processor.SinkProcessors;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * @see SinkProcessors#writeFile(String, DistributedFunction, Charset, boolean)
 */
public final class WriteFileP {

    private WriteFileP() { }

    /**
     * Use {@link SinkProcessors#writeFile(String, DistributedFunction, Charset, boolean)}
     */
    public static <T> ProcessorSupplier supplier(
            @Nonnull String directoryName,
            @Nonnull DistributedFunction<T, String> toStringFn,
            @Nonnull String charset,
            boolean append) {

        return SinkProcessors.writeBuffered(
                globalIndex -> createBufferedWriter(Paths.get(directoryName), globalIndex,
                        charset, append),
                (fileWriter, item) -> uncheckRun(() -> {
                    fileWriter.write(toStringFn.apply((T) item));
                    fileWriter.newLine();
                }),
                fileWriter -> uncheckRun(fileWriter::flush),
                fileWriter -> uncheckRun(fileWriter::close)
        );
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
            justification = "mkdirs() returns false if the directory already existed, which is good. "
                    + "We don't care even if it didn't exist and we failed to create it, "
                    + "because we'll fail later when trying to create the file.")
    private static BufferedWriter createBufferedWriter(Path directory, int globalIndex, String charset, boolean append) {
        directory.toFile().mkdirs();

        Path file = directory.resolve(String.valueOf(globalIndex));

        return uncheckCall(() -> Files.newBufferedWriter(file,
                Charset.forName(charset), StandardOpenOption.CREATE,
                append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING));
    }

}
