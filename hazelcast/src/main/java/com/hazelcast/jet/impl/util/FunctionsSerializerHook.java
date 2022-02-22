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

package com.hazelcast.jet.impl.util;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.BASIC_FUNCTIONS_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.BASIC_FUNCTIONS_DS_FACTORY_ID;

/**
 * Serialization hook for functions
 */
@SuppressWarnings("checkstyle:javadocvariable")
public class FunctionsSerializerHook implements DataSerializerHook {
    public static final int F_ID = FactoryIdHelper.getFactoryId(BASIC_FUNCTIONS_DS_FACTORY, BASIC_FUNCTIONS_DS_FACTORY_ID);

    public static final int FUNCTION_IDENTITY = 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case FUNCTION_IDENTITY:
                    return Util.Identity.INSTANCE;
                default:
                    throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
