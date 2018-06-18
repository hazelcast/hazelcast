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

package com.hazelcast.jet.avro;

import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedSupplier;
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
     * @param datumReaderSupplier the supplier of datum reader which reads
     *                            records from the files
     * @param <T>                 the type of the records
     */
    @Nonnull
    public static <T> AvroSourceBuilder<T> filesBuilder(
            @Nonnull String directory,
            @Nonnull DistributedSupplier<DatumReader<T>> datumReaderSupplier
    ) {
        return new AvroSourceBuilder<>(directory, datumReaderSupplier);
    }

    /**
     * Convenience for {@link #filesBuilder(String, DistributedSupplier)} which
     * reads all the files in the supplied directory as specific records using
     * supplied {@code recordClass}. If {@code recordClass} implements {@link
     * SpecificRecord}, {@link SpecificDatumReader} is used to read the records,
     * {@link ReflectDatumReader} is used otherwise.
     */
    @Nonnull
    public static <R> BatchSource<R> files(@Nonnull String directory, @Nonnull Class<R> recordClass) {
        return filesBuilder(directory, () -> SpecificRecord.class.isAssignableFrom(recordClass) ?
                new SpecificDatumReader<>(recordClass) : new ReflectDatumReader<>(recordClass)).build();
    }

    /**
     * Convenience for {@link #filesBuilder(String, DistributedSupplier)} which
     * reads all the files in the supplied directory as generic records and
     * emits the results of transforming each generic record with the supplied
     * mapping function.
     */
    @Nonnull
    public static <R> BatchSource<R> files(
            @Nonnull String directory,
            @Nonnull DistributedBiFunction<String, GenericRecord, R> mapOutputFn
    ) {
        return filesBuilder(directory, GenericDatumReader<GenericRecord>::new)
                .build(mapOutputFn);
    }
}
