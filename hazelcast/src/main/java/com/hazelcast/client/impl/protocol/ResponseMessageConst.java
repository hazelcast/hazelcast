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
public final class ResponseMessageConst {

    public static final short VOID = 1;
    public static final short BOOLEAN = 2;
    public static final short INTEGER = 3;
    public static final short LONG = 4;
    public static final short STRING = 5;
    public static final short DATA = 6;
    public static final short LIST_DATA = 7;
    public static final short MAP_INT_DATA = 8;
    public static final short MAP_DATA_DATA = 9;
    public static final short AUTHENTICATION = 10;
    public static final short PARTITIONS = 11;
    public static final short EXCEPTION = 12;
    public static final short DISTRIBUTED_OBJECT = 13;
    public static final short ENTRY_VIEW = 14;
}
