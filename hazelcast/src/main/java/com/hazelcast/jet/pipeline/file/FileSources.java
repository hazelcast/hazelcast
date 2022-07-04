/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
 * Contains factory methods for the Unified File Connector.
 *
 * @since Jet 4.4
 */
public final class FileSources {

    private FileSources() {
    }

    /**
     * The main entry point to the Unified File Connector.
     * <p>
     * Returns a {@link FileSourceBuilder} configured with default values, see
     * its documentation for more options.
     * <p>
     * The path specifies the filesystem type (for example {@code s3a://},
     * {@code hdfs://}) and the path to the files. If it doesn't specify a file
     * system, a local file system is used - in this case the path must be
     * absolute. By "local" we mean local to each Jet cluster member, not to
     * the client submitting the job.
     * <p>
     * The following file systems are supported:<ul>
     *     <li>{@code s3a://} (Amazon S3)
     *     <li>{@code hdfs://} (HDFS)
     *     <li>{@code wasbs://} (Azure Cloud Storage)
     *     <li>{@code adl://} (Azure Data Lake Gen 1)
     *     <li>{@code abfs://} (Azure Data Lake Gen 2)
     *     <li>{@code gs://} (Google Cloud Storage)
     * </ul>
     * <p>
     * The path must point to a directory. All files in the directory are
     * processed. Subdirectories are not processed recursively.
     * The path must not contain any wildcard characters.
     * <p>
     * Example usage:
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     *         p.readFrom(FileSources.files("/path/to/directory").build())
     *          .map(line -> LogParser.parse(line))
     *          .filter(log -> log.level().equals("ERROR"))
     *          .writeTo(Sinks.logger());
     * }</pre>
     *
     * @param path the path to the directory
     * @return the builder object with fluent API
     */
    public static FileSourceBuilder<String> files(String path) {
        return new FileSourceBuilder<>(path)
                .format(new LinesTextFileFormat());
    }
}
