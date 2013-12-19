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

package com.hazelcast.client;

import com.hazelcast.nio.serialization.*;

import java.util.Collection;
import java.util.Collections;

/**
 * @author mdogan 4/30/13
 */
public class ClientPortableHook implements PortableHook {

    public static final int ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CLIENT_PORTABLE_FACTORY, -3);

    public static final int GENERIC_ERROR = 1;
    public static final int AUTH = 2;
    public static final int PRINCIPAL = 3;
    public static final int GET_DISTRIBUTED_OBJECT_INFO = 4;
    public static final int DISTRIBUTED_OBJECT_INFO = 6;
    public static final int CREATE_PROXY = 7;
    public static final int DESTROY_PROXY = 8;
    public static final int LISTENER = 9;

    public int getFactoryId() {
        return ID;
    }

    public PortableFactory createFactory() {
        return new ClientPortableFactory();
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(ID, PRINCIPAL);
        builder.addUTFField("uuid").addUTFField("ownerUuid");
        return Collections.singleton(builder.build());
    }
}
