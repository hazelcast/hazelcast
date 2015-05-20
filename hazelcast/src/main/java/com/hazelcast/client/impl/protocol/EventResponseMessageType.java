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
public final class EventResponseMessageType {

    public static final short MEMBER_EVENT = 1;
    public static final short MEMBER_LIST_EVENT = 2;
    public static final short MEMBER_ATTRIBUTE_EVENT = 3;

    public static final short ENTRY_EVENT = 4;
    public static final short ITEM_EVENT = 5;
    public static final short TOPIC_EVENT = 6;
    public static final short PARTITION_LOST_EVENT = 7;


}
