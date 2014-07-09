/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.web;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class WebDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.WEB_DS_FACTORY, F_ID_OFFSET_WEBMODULE);

    public static final int SESSION_ATTRIBUTE_ID = 1;
    public static final int ADD_SESSION_ID = 2;
    public static final int DESTROY_SESSION_ID = 3;
    public static final int INVALIDATE_SESSION_ATTRIBUTES_ID = 4;
    public static final int REFERENCE_SESSION_ID = 5;

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {

            @Override
            public IdentifiedDataSerializable create(final int typeId) {
                switch (typeId) {
                    case SESSION_ATTRIBUTE_ID:
                        return new SessionAttributePredicate();
                    case ADD_SESSION_ID:
                        return new AddSessionEntryProcessor();
                    case DESTROY_SESSION_ID:
                        return new DestroySessionEntryProcessor();
                    case INVALIDATE_SESSION_ATTRIBUTES_ID:
                        return new InvalidateSessionAttributesEntryProcessor();
                    case REFERENCE_SESSION_ID:
                        return new ReferenceSessionEntryProcessor();
                    default:
                        throw new IllegalArgumentException();
                }
            }
        };
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }
}
