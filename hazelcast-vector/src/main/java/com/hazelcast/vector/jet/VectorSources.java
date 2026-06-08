/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.jet;

import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.jet.impl.VectorFiles;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.hazelcast.jet.pipeline.Sources.filesBuilder;

/**
 * Sources for loading and processing standard vector data formats.
 *
 * @since 5.5
 */
@Beta
public class VectorSources {
    private VectorSources() {
    }

    /**
     * Creates legacy file source in fvecs format. Provided for convenience,
     * if simpler API is sufficient. Assumes that the files are not shared by the members.
     *
     * @param directory directory containing files to read
     * @param glob mask for file names
     * @return fvecs file source
     * @see #fvecsFormat()
     */
    @Nonnull
    public static BatchSource<Map.Entry<Integer, VectorValues>> fvecs(@Nonnull String directory, @Nonnull String glob) {
        return filesBuilder(directory).glob(glob)
                .build(VectorFiles.fvecsFileFn(directory));
    }

    /**
     * Creates legacy file source in ivecs format. Provided for convenience,
     * if simpler API is sufficient. Assumes that the files are not shared by the members.
     *
     * @param directory directory containing files to read
     * @param glob mask for file names
     * @return ivecs file source
     * @see #ivecsFormat()
     */
    @Nonnull
    public static BatchSource<Map.Entry<Integer, int[]>> ivecs(@Nonnull String directory, @Nonnull String glob) {
        return filesBuilder(directory).glob(glob)
                .build(VectorFiles.ivecsFileFn(directory));
    }

    /**
     * Returns fvecs file format definition for use with Jet Unified File Connector
     * ({@link com.hazelcast.jet.pipeline.file.FileSources}).
     *
     * Example usage - load vectors to vector collection where key and metadata is vector index
     * in the fvecs file:
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * p.readFrom(FileSources.files("/path/to/directory")
     *                         .glob("*.fvecs")
     *                         .format(VectorSources.fvecsFormat())
     *                         .build())
     *   .writeTo(VectorSinks.vectorCollection("collection-name", Map.Entry::getKey, Map.Entry::getKey, Map.Entry::getValue));
     * }</pre>
     * @return fvecs format definition for use with Jet Unified File Connector
     * @see com.hazelcast.jet.pipeline.file.FileSources#files
     */
    public static FvecsFileFormat fvecsFormat() {
        return new FvecsFileFormat();
    }

    /**
     * Returns ivecs file format definition for use with Jet Unified File Connector
     * ({@link com.hazelcast.jet.pipeline.file.FileSources}).
     *
     * @return ivecs format definition for use with Jet Unified File Connector
     * @see com.hazelcast.jet.pipeline.file.FileSources#files
     */
    public static IvecsFileFormat ivecsFormat() {
        return new IvecsFileFormat();
    }
}
