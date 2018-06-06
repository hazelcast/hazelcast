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
import com.hazelcast.jet.pipeline.Sources;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nonnull;
import java.io.File;

/**
 * Contains factory methods for Apache Avro sources.
 */
public final class AvroSources {

    private AvroSources() {
    }

    /**
     * A source that reads records from Apache Avro files in a directory (but
     * not its subdirectories).
     * <p>
     * If {@code sharedFileSystem} is {@code true}, Jet will assume all members
     * see the same files. They will split the work so that each member will
     * read a part of the files. If {@code sharedFileSystem} is {@code false},
     * each member will read all files in the directory, assuming the are
     * local.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * Any {@code IOException} will cause the job to fail. The files must not
     * change while being read; if they do, the behavior is unspecified.
     * <p>
     * The default local parallelism for this processor is 2 (or 1 if just 1
     * CPU is available).
     *
     * @param directory           parent directory of the files
     * @param glob                the globbing mask, see {@link
     *                            java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *                            Use {@code "*"} for all files.
     * @param datumReaderSupplier the datum reader supplier
     * @param mapOutputFn         function to create output items. Parameters are
     *                            {@code fileName} and {@code W}.
     * @param sharedFileSystem    {@code true} if files are in a shared storage
     *                                        visible to all members, {@code
     *                                        false} otherwise
     * @param <W>                 the type of the records
     * @param <R>                 the type of the emitted value
     */
    @Nonnull
    public static <W, R> BatchSource<R> files(
            @Nonnull String directory,
            @Nonnull String glob,
            @Nonnull DistributedSupplier<DatumReader<W>> datumReaderSupplier,
            @Nonnull DistributedBiFunction<String, W, ? extends R> mapOutputFn,
            boolean sharedFileSystem
    ) {
        return Sources.batchFromProcessor("avroFilesSource(" + new File(directory, glob) + ')',
                AvroProcessors.readFilesP(directory, glob, datumReaderSupplier, mapOutputFn, sharedFileSystem));
    }

    /**
     * Convenience for {@link
     * #files(String, String, DistributedSupplier, DistributedBiFunction, boolean)}
     * which reads all the files in the supplied directory as specific records
     * using supplied {@code recordClass}. If {@code recordClass} implements
     * {@link SpecificRecord}, {@link SpecificDatumReader} is used to read the
     * records, {@link ReflectDatumReader} is used otherwise.
     */
    @Nonnull
    public static <R> BatchSource<R> files(
            @Nonnull String directory,
            @Nonnull Class<R> recordClass,
            boolean sharedFileSystem
    ) {
        return files(directory, "*", () -> SpecificRecord.class.isAssignableFrom(recordClass) ?
                        new SpecificDatumReader<>(recordClass) : new ReflectDatumReader<>(recordClass),
                (s, r) -> r, sharedFileSystem);
    }

    /**
     * Convenience for {@link
     * #files(String, String, DistributedSupplier, DistributedBiFunction, boolean)}
     * which reads all the files in the supplied directory as generic records and
     * emits the results of transforming each generic record with the supplied
     * mapping function.
     */
    @Nonnull
    public static <R> BatchSource<R> files(
            @Nonnull String directory,
            @Nonnull DistributedBiFunction<String, GenericRecord, R> mapOutputFn,
            boolean sharedFileSystem
    ) {
        return files(directory, "*", GenericDatumReader::new, mapOutputFn, sharedFileSystem);
    }
}
