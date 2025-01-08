/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

/**
 * Includes builtin connector names for phone home purposes. These connectors live in {@link Sources} and {@link Sinks}
 * <p>
 * The connector name format is "<connectorIdentifier>(Source|Sink)" and follows camel case.
 * e.g avroSink, fileSource, fileWatcherSource, avroSource and so on.
 */
public final class ConnectorNames {
    /**
     * {@link Sources#filesBuilder(String)} and {@link Sources#files(String)}
     */
    public static final String FILES = "fileSource";

    /**
     * {@link Sources#fileWatcher(String)}
     */
    public static final String FILE_WATCHER = "fileWatcherSource";

    private ConnectorNames() {
    }
}
