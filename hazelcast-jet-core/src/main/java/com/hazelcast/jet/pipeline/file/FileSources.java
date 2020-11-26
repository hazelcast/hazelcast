/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline.file;

/**
 * Convenience methods to FileSourceBuilder
 *
 * @since 4.4
 */
public final class FileSources {

    /**
     * Helper class
     */
    private FileSources() {
    }

    /**
     * The main entrypoint to the Unified File Connector
     * <p>
     * Returns a {@link FileSourceBuilder} configured with LinesTextFileFormat
     * - each line of the file is emitted from the source as a single String.
     * <p>
     * The path must point to a directory. All files in the directory are
     * processed. The directory is not processed recursively.
     * <p>
     * An optional glob parameter can be provided, e.g. {@code "file*"}, capturing {@code file1.txt},
     * {@code file2.txt}, ...
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * p.readFrom(FileSources.files("src/main/java"))
     *  .map(line -> LogParser.parse(line))
     *  .filter(log -> log.level().equals("ERROR"))
     *  .writeTo(Sinks.logger());}</pre>
     *
     * You can override the format by calling
     * {@link FileSourceBuilder#format(FileFormat)} method.
     * For example:
     * <pre>{@code
     * BatchSource<byte[]> source = FileSources.files("path/to/binary/file")
     *                                         .build();
     * }</pre>
     *
     * Usage:
     * <pre>{@code
     * BatchSource<byte[]> source = FileSources.files("path/to/binary/file")
     *                                         .build();
     * }</pre>
     *
     * @param path the path to the file
     *
     * @return FileSourceBuilder, which can be further modified using fluent API
     */
    public static FileSourceBuilder<String> files(String path) {
        return new FileSourceBuilder<>(path)
                .format(new LinesTextFileFormat());
    }
}
