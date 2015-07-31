/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.web.entryprocessor.DeleteSessionEntryProcessor;
import com.hazelcast.web.entryprocessor.GetAttributeEntryProcessor;
import com.hazelcast.web.entryprocessor.GetAttributeNamesEntryProcessor;
import com.hazelcast.web.entryprocessor.GetSessionStateEntryProcessor;
import com.hazelcast.web.entryprocessor.SessionUpdateEntryProcessor;

public class WebDataSerializerHook implements DataSerializerHook {

    /**
     * The constant F_ID.
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.WEB_DS_FACTORY, F_ID_OFFSET_WEBMODULE);

    /**
     * The constant SESSION_UPDATE.
     */
    public static final int SESSION_UPDATE = 1;
    /**
     * The constant SESSION_DELETE.
     */
    public static final int SESSION_DELETE = 2;
    /**
     * The constant GET_ATTRIBUTE.
     */
    public static final int GET_ATTRIBUTE = 3;
    /**
     * The constant GET_ATTRIBUTE_NAMES.
     */
    public static final int GET_ATTRIBUTE_NAMES = 4;
    /**
     * The constant GET_SESSION_STATE.
     */
    public static final int GET_SESSION_STATE = 5;
    /**
     * The constant SESSION_STATE.
     */
    public static final int SESSION_STATE = 6;

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(final int typeId) {
                return getIdentifiedDataSerializable(typeId);
            }
        };
    }

    private IdentifiedDataSerializable getIdentifiedDataSerializable(int typeId) {
        IdentifiedDataSerializable dataSerializable;
        switch (typeId) {
            case SESSION_UPDATE:
                dataSerializable = new SessionUpdateEntryProcessor();
                break;
            case SESSION_DELETE:
                dataSerializable = new DeleteSessionEntryProcessor();
                break;
            case GET_ATTRIBUTE:
                dataSerializable = new GetAttributeEntryProcessor();
                break;
            case GET_ATTRIBUTE_NAMES:
                dataSerializable = new GetAttributeNamesEntryProcessor();
                break;
            case GET_SESSION_STATE:
                dataSerializable = new GetSessionStateEntryProcessor();
                break;
            case SESSION_STATE:
                dataSerializable =  new SessionState();
                break;
            default:
                dataSerializable = null;
        }
        return dataSerializable;
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }
}
