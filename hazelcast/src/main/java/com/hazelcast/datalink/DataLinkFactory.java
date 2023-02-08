/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datalink;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.spi.annotation.Beta;

/**
 * Creates data link. Configuration is provided by {@link #init(DataLinkConfig)}.
 *
 * @param <DL> type of the data link
 * @since 5.2
 */
@Beta
public interface DataLinkFactory<DL> extends AutoCloseable {

    /**
     * Returns configured data link. Depending on configuration and implementation it can create new data link
     * or reuse existing one. The implementation <i>must</i> be thread-safe, since this method may be called
     * by multiple threads concurrently.
     */
    DL getDataLink();

    /**
     * Initialize factory with the config
     *
     * @param config configuration of the given data link
     */
    void init(DataLinkConfig config);

    /**
     * Test connection of previously initialized data link.
     *
     * @since 5.3
     */
    boolean testConnection() throws Exception;

    /**
     * Closes underlying resources
     *
     * @throws Exception
     */
    @Override
    default void close() throws Exception {
        //no op
    }
}
