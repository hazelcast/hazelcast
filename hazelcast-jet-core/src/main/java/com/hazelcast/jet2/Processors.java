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

package com.hazelcast.jet2;

import com.hazelcast.jet2.impl.IListReader;
import com.hazelcast.jet2.impl.IListWriter;
import com.hazelcast.jet2.impl.IMapReader;
import com.hazelcast.jet2.impl.IMapWriter;

/**
 * Contains various predefined processors
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
     * @return processors for  writing to a Hazelcast IMap
     */
    public static ProcessorMetaSupplier mapWriter(String mapName) {
        return IMapWriter.supplier(mapName);
    }

    /**
     * @return processors for reading from a Hazelcast IList
     */
    public static ProcessorMetaSupplier listReader(String listName) {
        return IListReader.supplier(listName);
    }

    /**
     * @return a processor for writing to a Hazelcast IList
     */
    public static ProcessorSupplier listWriter(String listName) {
        return IListWriter.supplier(listName);
    }
}
