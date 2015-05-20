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

package com.hazelcast.client.impl.protocol;

/**
 * Client Message type is the unique id defines the type of message.
 */
public enum ResponseMessageType {

    VOID(ResponseMessageConst.VOID),
    BOOLEAN(ResponseMessageConst.BOOLEAN),
    INTEGER(ResponseMessageConst.INTEGER),
    LONG(ResponseMessageConst.LONG),
    STRING(ResponseMessageConst.STRING),
    DATA(ResponseMessageConst.DATA),
    AUTHENTICATION(ResponseMessageConst.AUTHENTICATION),
    LIST_DATA(ResponseMessageConst.LIST_DATA),
    MAP_INT_DATA(ResponseMessageConst.MAP_INT_DATA),
    MAP_DATA_DATA(ResponseMessageConst.MAP_DATA_DATA),
    PARTITIONS(ResponseMessageConst.PARTITIONS);

    private final int id;

    ResponseMessageType(int messageType) {
        this.id = messageType;
    }

    public int id() {
        return id;
    }


}
