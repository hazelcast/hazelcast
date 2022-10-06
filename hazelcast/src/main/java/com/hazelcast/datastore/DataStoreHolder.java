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

package com.hazelcast.datastore;

import com.hazelcast.spi.annotation.Beta;

import java.util.function.Supplier;

/**
 * Holder of a data store and which handles the close of the wrapped datastore
 *
 * @param <DS> - Data store type hold by the object of this class
 * @since 5.2
 */
@Beta
public interface DataStoreHolder<DS> extends Supplier<DS>, AutoCloseable {

    static <DS> DataStoreHolder<DS> closing(DS datastore) {
        return new DataStoreHolder<DS>() {
            @Override
            public DS get() {
                return datastore;
            }

            @Override
            public void close() throws Exception {
                if (datastore instanceof AutoCloseable) {
                    ((AutoCloseable) datastore).close();
                }
            }
        };
    }

    static <DS> DataStoreHolder<DS> nonClosing(DS datastore) {
        return new DataStoreHolder<DS>() {
            @Override
            public DS get() {
                return datastore;
            }

            @Override
            public void close() {
                //no closing
            }
        };
    }
}
