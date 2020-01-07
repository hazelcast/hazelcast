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

package com.hazelcast.map.impl;

import com.hazelcast.internal.serialization.Data;

import java.io.IOException;

/**
 * MetadataInitializer is used to process and generate metadata from
 * keys and values saved in map store.
 */
public interface MetadataInitializer {

    /**
     * Returns metadata for given binary data. Implementing class decides
     * the type of the metadata returned.
     *
     * @param keyData
     * @return metadata created from given binary data
     * @throws IOException
     */
    Object createFromData(Data keyData) throws IOException;

    /**
     * Returns metadata for given object. Implementing class decides
     * the type of the metadata returned.
     *
     * It is up to the implementating class to verify the type of the
     * argument.
     *
     * @param object
     * @return metadata created from given object
     * @throws IOException
     */
    Object createFromObject(Object object) throws IOException;
}
