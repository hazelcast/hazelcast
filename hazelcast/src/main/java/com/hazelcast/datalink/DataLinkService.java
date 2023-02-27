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
 * DataLinkService provides access to existing {@link DataLink}s.
 * <p>
 * DataLinks can be configured via one of the following:
 * <ul>
 * <li>statically in the configuration</li>
 * <li>dynamically via {@link com.hazelcast.config.Config#addDataLinkConfig(DataLinkConfig)}</li>
 * <li>via SQL {@code CREATE DATA LINK ...} command</li>
 * </ul>
 * @since 5.3
 */
@Beta
public interface DataLinkService {

    /**
     * Returns DataLink with the given name.
     * <p>
     * Type checked against the provided clazz parameter.
     * <p>
     * The callers must call {@link DataLink#close()} after obtaining required
     * resources (e.g. connections) from the data link to ensure correct
     * cleanup of resources.
     *
     * @param name  name of the DataLink
     * @param clazz type of the DataLink
     */
    <T extends DataLink> T getDataLink(String name, Class<T> clazz);

}
