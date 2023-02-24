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
 * @since 5.2
 */
@Beta
public interface DataLink extends AutoCloseable {

    /**
     * Returns the name of this DataLink as specified in the
     * {@link DataLinkConfig} or given to the {@code CREATE DATA LINK}
     * command.
     *
     * @return the name of this DataLink
     */
    String getName();

    /**
     * Returns the configuration of this data link
     */
    DataLinkConfig getConfig();

    void retain();

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
