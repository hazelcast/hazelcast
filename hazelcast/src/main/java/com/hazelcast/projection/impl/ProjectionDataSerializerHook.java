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

package com.hazelcast.projection.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.function.Supplier;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PROJECTION_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PROJECTION_DS_FACTORY_ID;

public final class ProjectionDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(PROJECTION_DS_FACTORY, PROJECTION_DS_FACTORY_ID);

    public static final int SINGLE_ATTRIBUTE = 0;
    public static final int MULTI_ATTRIBUTE = 1;
    public static final int IDENTITY_PROJECTION = 2;

    private static final int LEN = IDENTITY_PROJECTION + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];

        constructors[SINGLE_ATTRIBUTE] = SingleAttributeProjection::new;
        constructors[MULTI_ATTRIBUTE] = MultiAttributeProjection::new;
        constructors[IDENTITY_PROJECTION] = () -> IdentityProjection.INSTANCE;

        return new ArrayDataSerializableFactory(constructors);
    }
}
