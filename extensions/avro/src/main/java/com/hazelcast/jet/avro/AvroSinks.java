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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nonnull;

/**
 * Contains factory methods for Apache Avro sinks.
 *
 * @since Jet 3.0
 */
public final class AvroSinks {

    private AvroSinks() {
    }

    /**
     * Returns a sink that that writes the items it receives to Apache Avro
     * files. Each processor will write to its own file whose name is equal to
     * the processor's global index (an integer unique to each processor of the
     * vertex), but a single pathname is used to resolve the containing
     * directory of all files, on all cluster members. The sink always
     * overwrites the files.
     * <p>
     * The sink creates a {@link DataFileWriter} for each processor using the
     * supplied {@code datumWriterSupplier} with the given {@link Schema}.
     * <p>
     * No state is saved to snapshot for this sink. After the job is restarted,
     * the items will be missing since files will be overwritten.
     * <p>
     * The default local parallelism for this sink is 1.
     *
     * @param directoryName directory to create the files in. Will be created
     *                      if it doesn't exist. Must be the same on all members.
     * @param schema the record schema
     * @param datumWriterSupplier the record writer supplier
     * @param <R> the type of the record
     */
    @Nonnull
    public static <R> Sink<R> files(
            @Nonnull String directoryName,
            @Nonnull Schema schema,
            @Nonnull SupplierEx<DatumWriter<R>> datumWriterSupplier
    ) {

        return Sinks.fromProcessor("avroFilesSink(" + directoryName + ')',
                AvroProcessors.writeFilesP(directoryName, schema, datumWriterSupplier));
    }

    /**
     * Convenience for {@link #files(String, Schema,
     * SupplierEx)} which uses either {@link SpecificDatumWriter} or
     * {@link ReflectDatumWriter} depending on the supplied {@code recordClass}.
     */
    @Nonnull
    public static <R> Sink<R> files(
            @Nonnull String directoryName,
            @Nonnull Class<R> recordClass,
            @Nonnull Schema schema
    ) {
        return files(directoryName, schema, () -> SpecificRecord.class.isAssignableFrom(recordClass) ?
                new SpecificDatumWriter<>(recordClass) : new ReflectDatumWriter<>(recordClass));
    }

    /**
     * Convenience for {@link #files(String, Schema,
     * SupplierEx)} which uses {@link GenericDatumWriter}.
     */
    @Nonnull
    public static Sink<IndexedRecord> files(
            @Nonnull String directoryName,
            @Nonnull Schema schema
    ) {
        return files(directoryName, schema, GenericDatumWriter::new);
    }
}
