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

import com.hazelcast.config.Config;
import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.core.ProcessorMetaSupplier;

import javax.annotation.Nonnull;

/**
 * The interface provides access to {@link DataLink}s for Jet processors. A Jet
 * processor can obtain a reference to the service via {@link
 * ProcessorMetaSupplier.Context#dataLinkService()}.
 * <p>
 * DataLinks can be created via one of the following ways:
 * <ul>
 *     <li>statically in the configuration
 *     <li>dynamically via {@link Config#addDataLinkConfig(DataLinkConfig)}
 *     <li>via SQL {@code CREATE DATA LINK ...} command
 * </ul>
 *
 * @since 5.3
 */
public interface DataLinkService {

    /**
     * Returns {@link DataLink} with the given name and `retain` it (calls
     * {@link DataLink#retain()}). The caller is responsible for calling
     * {@link DataLink#release()} after it is done with the DataLink.
     * <p>
     * Type is checked against the provided `clazz` argument.
     *
     * @param name  name of the DataLink
     * @param clazz expected type of the DataLink
     *
     * @throws HazelcastException if the requested DataLink doesn't exist, or has
     *     a different type than `clazz`
     */
    @Nonnull
    <T extends DataLink> T getAndRetainDataLink(String name, Class<T> clazz);

}
