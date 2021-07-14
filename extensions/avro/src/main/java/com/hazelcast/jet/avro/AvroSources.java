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
import com.hazelcast.jet.pipeline.BatchSource;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nonnull;

/**
 * Contains factory methods for Apache Avro sources.
 *
 * @since Jet 3.0
 */
public final class AvroSources {

    private AvroSources() {
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom Avro file source for the Pipeline API. The source reads records
     * from Apache Avro files in a directory (but not its subdirectories).
     *
     * @param directory           parent directory of the files
     * @param recordClass         the class to read
     * @param <D>                 the type of the datum
     */
    @Nonnull
    public static <D> AvroSourceBuilder<D> filesBuilder(
            @Nonnull String directory,
            @Nonnull Class<D> recordClass
    ) {
        return filesBuilder(directory, () -> SpecificRecord.class.isAssignableFrom(recordClass) ?
                new SpecificDatumReader<>(recordClass) : new ReflectDatumReader<>(recordClass));
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom Avro file source for the Pipeline API. The source reads records
     * from Apache Avro files in a directory (but not its subdirectories).
     *
     * @param directory           parent directory of the files
     * @param datumReaderSupplier the supplier of datum reader which reads
     *                            records from the files
     * @param <D>                 the type of the datum
     */
    @Nonnull
    public static <D> AvroSourceBuilder<D> filesBuilder(
            @Nonnull String directory,
            @Nonnull SupplierEx<? extends DatumReader<D>> datumReaderSupplier
    ) {
        return new AvroSourceBuilder<>(directory, datumReaderSupplier);
    }

    /**
     * Convenience for {@link #filesBuilder(String, Class)} which
     * reads all the files in the supplied directory as specific records using
     * supplied {@code datumClass}. If {@code datumClass} implements {@link
     * SpecificRecord}, {@link SpecificDatumReader} is used to read the records,
     * {@link ReflectDatumReader} is used otherwise.
     */
    @Nonnull
    public static <D> BatchSource<D> files(@Nonnull String directory, @Nonnull Class<D> datumClass) {
        return filesBuilder(directory, datumClass).build();
    }

    /**
     * Convenience for {@link #filesBuilder(String, SupplierEx)} which
     * reads all the files in the supplied directory as generic records and
     * emits the results of transforming each generic record with the supplied
     * mapping function.
     */
    @Nonnull
    public static <D> BatchSource<D> files(
            @Nonnull String directory,
            @Nonnull BiFunctionEx<String, GenericRecord, D> mapOutputFn
    ) {
        return filesBuilder(directory, GenericDatumReader<GenericRecord>::new)
                .build(mapOutputFn);
    }
}
