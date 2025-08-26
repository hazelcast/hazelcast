/*
 * Copyright 2025 Hazelcast Inc.
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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
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
import java.io.Serial;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Permission;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.avro.AvroSinks.AVRO_SINK_CONNECTOR_NAME;
import static com.hazelcast.jet.avro.AvroSources.AVRO_SOURCE_CONNECTOR_NAME;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static com.hazelcast.security.permission.ActionConstants.ACTION_WRITE;
import static java.util.Collections.singletonList;

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
        FunctionEx<? super Path, ? extends Stream<T>> readFileFn =
                dataFileReadFn(directory, datumReaderSupplier, mapOutputFn);
        return ReadFilesP.metaSupplier(directory, glob, sharedFileSystem, true, readFileFn, AVRO_SOURCE_CONNECTOR_NAME);
    }

    /**
     * Returns a supplier of processors for {@link AvroSinks#files}.
     */
    @Nonnull
    public static <D> ProcessorMetaSupplier writeFilesP(
            @Nonnull String directoryName,
            @Nonnull Schema schema,
            @Nonnull SupplierEx<DatumWriter<D>> datumWriterSupplier) {
        return writeFilesP(directoryName, schema, datumWriterSupplier, AVRO_SINK_CONNECTOR_NAME);
    }

    /**
     * Returns a supplier of processors for {@link AvroSinks#files}.
     */
    @Nonnull
    public static <D> ProcessorMetaSupplier writeFilesP(
            @Nonnull String directoryName,
            @Nonnull Schema schema,
            @Nonnull SupplierEx<DatumWriter<D>> datumWriterSupplier,
            String connectorName
    ) {
        String jsonSchema = schema.toString();
        return preferLocalParallelismOne(
                ConnectorPermission.file(directoryName, ACTION_WRITE),
                WriteBufferedP.<DataFileWriter<D>, D>supplier(
                        dataFileWriterFn(directoryName, jsonSchema, datumWriterSupplier),
                        DataFileWriter::append,
                        DataFileWriter::flush,
                        DataFileWriter::close
                ), connectorName);
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
            justification = "mkdirs() returns false if the directory already existed, which is good. "
                    + "We don't care even if it didn't exist and we failed to create it, "
                    + "because we'll fail later when trying to create the file.")
    private static <D> FunctionEx<Processor.Context, DataFileWriter<D>> dataFileWriterFn(
            String directoryName, String jsonSchema, SupplierEx<DatumWriter<D>> datumWriterSupplier
    ) {
        return new FunctionEx<>() {

            @Serial
            private static final long serialVersionUID = 1L;

            @Override
            public DataFileWriter<D> applyEx(Processor.Context context) throws Exception {
                Schema.Parser parser = new Schema.Parser();
                Schema schema = parser.parse(jsonSchema);

                Path directory = Paths.get(directoryName);
                directory.toFile().mkdirs();

                Path file = directory.resolve(String.valueOf(context.globalProcessorIndex()));
                DataFileWriter<D> writer = new DataFileWriter<>(datumWriterSupplier.get());
                writer.create(schema, file.toFile());
                return writer;
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(ConnectorPermission.file(directoryName, ACTION_WRITE));
            }
        };
    }

    private static <D, T> FunctionEx<? super Path, ? extends Stream<T>> dataFileReadFn(
            String directoryName,
            SupplierEx<? extends DatumReader<D>> datumReaderSupplier,
            BiFunctionEx<String, ? super D, T> mapOutputFn
    ) {
        return new FunctionEx<>() {

            @Serial
            private static final long serialVersionUID = 1L;

            @Override
            public Stream<T> applyEx(Path path) throws Exception {
                DataFileReader<D> reader = new DataFileReader<>(path.toFile(), datumReaderSupplier.get());
                String fileName = path.getFileName().toString();
                return StreamSupport.stream(reader.spliterator(), false)
                        .map(item -> mapOutputFn.apply(fileName, item))
                        .onClose(() -> uncheckRun(reader::close));
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(ConnectorPermission.file(directoryName, ACTION_READ));
            }
        };
    }
}
