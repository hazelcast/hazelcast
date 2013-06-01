/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.serialization.*;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.util.Collection;

/**
 * @mdogan 4/30/13
 */
public final class SpiPortableHook implements PortableHook {

    public static int ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.SPI_PORTABLE_FACTORY, -1);

    public static final int USERNAME_PWD_CRED = 1;
    public static final int COLLECTION = 2;
    public static final int ITEM_EVENT = 3;
    public static final int ENTRY_EVENT = 4;

    public int getFactoryId() {
        return ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory() {
            public Portable create(int classId) {
                switch (classId){
                    case USERNAME_PWD_CRED:
                        return new UsernamePasswordCredentials();
                    case COLLECTION:
                        return new PortableCollection();
                    case ITEM_EVENT:
                        return new PortableItemEvent();
                    case ENTRY_EVENT:
                        return new PortableEntryEvent();
                }
                return null;
            }
        };
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
