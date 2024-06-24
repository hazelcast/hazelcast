/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.map.impl.operation.EECompatMapChunk;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_MAP_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_MAP_DS_FACTORY_ID;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Hook that registers {@link  EECompatMapChunk} for OS to EE restart
 */
public final class EECompatMapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_MAP_DS_FACTORY, ENTERPRISE_MAP_DS_FACTORY_ID);

    public static final int ENTERPRISE_MAP_CHUNK = 10;

    private static final String FACTORY_ID = "com.hazelcast.DataSerializerHook";
    private static final int LEN = ENTERPRISE_MAP_CHUNK + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];

        constructors[ENTERPRISE_MAP_CHUNK] = EECompatMapChunk::new;

        return new ArrayDataSerializableFactory(constructors);
    }

    @Override
    public boolean shouldRegister() {
        try {
            Iterator<DataSerializerHook> hookIterator = ServiceLoader.iterator(DataSerializerHook.class, FACTORY_ID,
                    this.getClass().getClassLoader());

            Optional<String> eeMapDataSerializer = StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(hookIterator, Spliterator.ORDERED),
                            false)
                    .map(hook -> hook.getClass().getName())
                    .filter(s -> s.equals("com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook"))
                    .findAny();
            // This hook should be registered only if
            // - the EnterpriseMapDataSerializerHook is not present,
            // - and we are NOT running EE
            return eeMapDataSerializer.isEmpty() && !BuildInfoProvider.getBuildInfo().isEnterprise();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
