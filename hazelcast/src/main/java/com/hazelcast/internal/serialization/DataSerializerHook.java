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

package com.hazelcast.internal.serialization;

import com.hazelcast.nio.serialization.DataSerializableFactory;

import java.util.Map;

public interface DataSerializerHook {

    int F_ID_OFFSET_WEBMODULE = -1000;
    int F_ID_OFFSET_HIBERNATE = -2000;

    int getFactoryId();

    DataSerializableFactory createFactory();

    /**
     * Gives the hook a chance to further configure factories after all
     * factories were created. It can be used for configuring constructors for
     * classes in submodules, for which the factoryId and classId cannot be
     * changed.
     */
    default void afterFactoriesCreated(Map<Integer, DataSerializableFactory> factories) {
        // nothing by default
    }
}
