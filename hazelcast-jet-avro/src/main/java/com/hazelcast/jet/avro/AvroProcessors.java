/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.avro;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.connector.WriteBufferedP;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * Static utility class with factories of Apache Avro source and sink
 * processors.
 *
 * @since 3.0
 */
public final class AvroProcessors {

    private AvroProcessors() {
    }

    /**
     * Returns a supplier of processors for {@link AvroSources#filesBuilder}.
     */
    @Nonnull
    public static <D, T> ProcessorMetaSupplier readFilesP(
            @Nonnull String directory,
            @Nonnull String glob,
            boolean sharedFileSystem,
            @Nonnull SupplierEx<? extends DatumReader<D>> datumReaderSupplier,
            @Nonnull BiFunctionEx<String, ? super D, T> mapOutputFn
    ) {
        return ReadFilesP.metaSupplier(directory, glob, sharedFileSystem,
                path -> {
                    DataFileReader<D> reader = new DataFileReader<>(path.toFile(), datumReaderSupplier.get());
                    return StreamSupport.stream(reader.spliterator(), false)
                                        .onClose(() -> uncheckRun(reader::close));
                },
                mapOutputFn);
    }

    /**
     * Returns a supplier of processors for {@link AvroSinks#files}.
     */
    @Nonnull
    public static <D> ProcessorMetaSupplier writeFilesP(
            @Nonnull String directoryName,
            @Nonnull SupplierEx<Schema> schemaSupplier,
            @Nonnull SupplierEx<DatumWriter<D>> datumWriterSupplier
    ) {
        return preferLocalParallelismOne(WriteBufferedP.<DataFileWriter<D>, D>supplier(
                        context -> createWriter(Paths.get(directoryName), context.globalProcessorIndex(),
                                schemaSupplier, datumWriterSupplier),
                        DataFileWriter::append,
                        DataFileWriter::flush,
                        DataFileWriter::close
                ));
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
            justification = "mkdirs() returns false if the directory already existed, which is good. "
                    + "We don't care even if it didn't exist and we failed to create it, "
                    + "because we'll fail later when trying to create the file.")
    private static <D> DataFileWriter<D> createWriter(
            Path directory, int globalIndex,
            SupplierEx<Schema> schemaSupplier,
            SupplierEx<DatumWriter<D>> datumWriterSupplier
    ) throws IOException {
        directory.toFile().mkdirs();

        Path file = directory.resolve(String.valueOf(globalIndex));

        DataFileWriter<D> writer = new DataFileWriter<>(datumWriterSupplier.get());
        writer.create(schemaSupplier.get(), file.toFile());
        return writer;
    }
}
