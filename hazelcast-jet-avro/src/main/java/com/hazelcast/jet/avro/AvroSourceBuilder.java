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

import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.BatchSource;
import org.apache.avro.io.DatumReader;

import javax.annotation.Nonnull;
import java.io.File;

import static com.hazelcast.jet.pipeline.Sources.batchFromProcessor;

/**
 * Builder for an Avro file source which reads records from Avro files in a
 * directory (but not its subdirectories) and emits output object created by
 * {@code mapOutputFn}.
 *
 * @param <D> the type of the datum read by {@code datumReaderSupplier}
 */
public final class AvroSourceBuilder<D> {

    private static final String GLOB_WILDCARD = "*";

    private final String directory;

    private String glob = GLOB_WILDCARD;
    private boolean sharedFileSystem;

    private final SupplierEx<? extends DatumReader<D>> datumReaderSupplier;

    /**
     * Use {@link AvroSources#filesBuilder}.
     */
    AvroSourceBuilder(
            @Nonnull String directory,
            @Nonnull SupplierEx<? extends DatumReader<D>> datumReaderSupplier
    ) {
        this.directory = directory;
        this.datumReaderSupplier = datumReaderSupplier;
    }

    /**
     * Sets the globbing mask, see {@link
     * java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     * Default value is {@code "*"} which means all files.
     */
    public AvroSourceBuilder<D> glob(@Nonnull String glob) {
        this.glob = glob;
        return this;
    }

    /**
     * Sets if files are in a shared storage visible to all members. Default
     * value is {@code false}
     * <p>
     * If {@code sharedFileSystem} is {@code true}, Jet will assume all members
     * see the same files. They will split the work so that each member will
     * read a part of the files. If {@code sharedFileSystem} is {@code false},
     * each member will read all files in the directory, assuming the are
     * local.
     */
    public AvroSourceBuilder<D> sharedFileSystem(boolean sharedFileSystem) {
        this.sharedFileSystem = sharedFileSystem;
        return this;
    }

    /**
     * Builds a custom Avro file {@link BatchSource} with supplied components
     * and the output function {@code mapOutputFn}.
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
     * @param mapOutputFn the function which creates output object from each
     *                    record. Gets the filename and record read by {@code
     *                    datumReader} as parameters
     * @param <T>         the type of the items the source emits
     */
    public <T> BatchSource<T> build(@Nonnull BiFunctionEx<String, ? super D, T> mapOutputFn) {
        return batchFromProcessor("avroFilesSource(" + new File(directory, glob) + ')',
                AvroProcessors.readFilesP(directory, glob, sharedFileSystem, datumReaderSupplier, mapOutputFn));
    }

    /**
     * Convenience for {@link AvroSourceBuilder#build(BiFunctionEx)}.
     * Source emits records read by {@code datumReader} to downstream without
     * any transformation.
     */
    public BatchSource<D> build() {
        return build((filename, datum) -> datum);
    }
}
