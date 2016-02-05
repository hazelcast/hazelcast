/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.api.application.localization;

import java.util.Map;
import java.io.IOException;

import com.hazelcast.jet.impl.application.localization.Chunk;
import com.hazelcast.jet.spi.JetException;
import com.hazelcast.jet.impl.application.LocalizationResourceDescriptor;
import com.hazelcast.jet.impl.application.localization.classloader.ResourceStream;

/**
 * Interface for localization storage
 * <p/>
 * It stores byte-code of classes which will be used in application
 */
public interface LocalizationStorage {
    /**
     * Add next chunk with byte-code into the storage
     *
     * @param chunk -   chunk with byte-code
     * @throws IOException
     * @throws JetException
     */
    void receiveFileChunk(Chunk chunk) throws IOException, JetException;

    /**
     * Accepts localisation phase.
     * Signal that no more chunks will be received
     *
     * @throws InvalidLocalizationException
     */
    void accept() throws InvalidLocalizationException;

    /**
     * Returns classLoaders corresponding to the received byte-code
     *
     * @return
     */
    ClassLoader getClassLoader();

    /**
     * @return all resources stored in storage
     * @throws IOException
     */
    Map<LocalizationResourceDescriptor, ResourceStream> getResources() throws IOException;

    /**
     * Clean-up all data in storage
     */
    void cleanUp();
}
