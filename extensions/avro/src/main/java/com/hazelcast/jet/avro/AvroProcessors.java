/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.avro;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.connector.WriteBufferedP;
import com.hazelcast.security.permission.ConnectorPermission;
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
import static com.hazelcast.security.permission.ActionConstants.ACTION_WRITE;

/**
 * Static utility class with factories of Apache Avro source and sink
 * processors.
 *
 * @since Jet 3.0
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
        return ReadFilesP.metaSupplier(directory, glob, sharedFileSystem, true,
                path -> {
                    DataFileReader<D> reader = new DataFileReader<>(path.toFile(), datumReaderSupplier.get());
                    String fileName = path.getFileName().toString();
                    return StreamSupport.stream(reader.spliterator(), false)
                                        .map(item -> mapOutputFn.apply(fileName, item))
                                        .onClose(() -> uncheckRun(reader::close));
                });
    }

    /**
     * Returns a supplier of processors for {@link AvroSinks#files}.
     */
    @Nonnull
    public static <D> ProcessorMetaSupplier writeFilesP(
            @Nonnull String directoryName,
            @Nonnull Schema schema,
            @Nonnull SupplierEx<DatumWriter<D>> datumWriterSupplier
    ) {
        String jsonSchema = schema.toString();
        return preferLocalParallelismOne(WriteBufferedP.<DataFileWriter<D>, D>supplier(
                        context -> createWriter(Paths.get(directoryName), context.globalProcessorIndex(),
                                jsonSchema, datumWriterSupplier),
                        DataFileWriter::append,
                        DataFileWriter::flush,
                        DataFileWriter::close,
                        () -> ConnectorPermission.file(directoryName, ACTION_WRITE)
                ));
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
            justification = "mkdirs() returns false if the directory already existed, which is good. "
                    + "We don't care even if it didn't exist and we failed to create it, "
                    + "because we'll fail later when trying to create the file.")
    private static <D> DataFileWriter<D> createWriter(
            Path directory, int globalIndex,
            String jsonSchema,
            SupplierEx<DatumWriter<D>> datumWriterSupplier
    ) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(jsonSchema);

        directory.toFile().mkdirs();

        Path file = directory.resolve(String.valueOf(globalIndex));
        DataFileWriter<D> writer = new DataFileWriter<>(datumWriterSupplier.get());
        writer.create(schema, file.toFile());
        return writer;
    }
}
