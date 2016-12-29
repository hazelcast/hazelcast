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

package com.hazelcast.jet;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.impl.connector.AbstractProducer;
import com.hazelcast.jet.impl.connector.IListReader;
import com.hazelcast.jet.impl.connector.IListWriter;
import com.hazelcast.jet.impl.connector.IMapReader;
import com.hazelcast.jet.impl.connector.IMapWriter;

/**
 * Static utility class with factory methods for a number of predefined processors.
 */
public final class Processors {

    private Processors() {
    }

    /**
     * @return processors for partitioned reading from a Hazelcast IMap
     */
    public static ProcessorMetaSupplier mapReader(String mapName) {
        return IMapReader.supplier(mapName);
    }

    /**
     * @return processors for partitioned reading from a remote Hazelcast IMap
     */
    public static ProcessorMetaSupplier mapReader(String mapName, ClientConfig clientConfig) {
        return IMapReader.supplier(mapName, clientConfig);
    }

    /**
     * @return processors for  writing to a Hazelcast IMap
     */
    public static ProcessorMetaSupplier mapWriter(String mapName) {
        return IMapWriter.supplier(mapName);
    }

    /**
     * @return processors for  writing to a remote Hazelcast IMap
     */
    public static ProcessorMetaSupplier mapWriter(String mapName, ClientConfig clientConfig) {
        return IMapWriter.supplier(mapName, clientConfig);
    }

    /**
     * @return processors for reading from a Hazelcast IList
     */
    public static ProcessorMetaSupplier listReader(String listName) {
        return IListReader.supplier(listName);
    }

    /**
     * @return processors for reading from a remote Hazelcast IList
     */
    public static ProcessorMetaSupplier listReader(String listName, ClientConfig clientConfig) {
        return IListReader.supplier(listName, clientConfig);
    }

    /**
     * @return a processor for writing to a Hazelcast IList
     */
    public static ProcessorSupplier listWriter(String listName) {
        return IListWriter.supplier(listName);
    }

    /**
     * @return a processor for writing to a remote Hazelcast IList
     */
    public static ProcessorSupplier listWriter(String listName, ClientConfig clientConfig) {
        return IListWriter.supplier(listName, clientConfig);
    }

    public static class NoopProducer extends AbstractProducer {
    }
}
