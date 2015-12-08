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
 * Message type ids of event responses in client protocol. They also used to bind a request to event inside Request
 * annotation.
 * <p/>
 * Event response classes are defined    {@link com.hazelcast.client.impl.protocol.template.EventResponseTemplate}
 * <p/>
 * see {@link   com.hazelcast.client.impl.protocol.template.ClientMessageTemplate#addMembershipListener(boolean)} ()}
 * for  a sample usage of events in a request.
 */
public final class EventMessageConst {

    public static final int EVENT_MEMBER = 200;
    public static final int EVENT_MEMBERLIST = 201;
    public static final int EVENT_MEMBERATTRIBUTECHANGE = 202;
    public static final int EVENT_ENTRY = 203;
    public static final int EVENT_ITEM = 204;
    public static final int EVENT_TOPIC = 205;
    public static final int EVENT_PARTITIONLOST = 206;
    public static final int EVENT_DISTRIBUTEDOBJECT = 207;
    public static final int EVENT_CACHEINVALIDATION = 208;
    public static final int EVENT_MAPPARTITIONLOST = 209;
    public static final int EVENT_CACHE = 210;
    public static final int EVENT_CACHEBATCHINVALIDATION = 211;
    //ENTERPRISE
    public static final int EVENT_QUERYCACHESINGLE = 212;
    public static final int EVENT_QUERYCACHEBATCH = 213;

    public static final int EVENT_CACHEPARTITIONLOST = 214;
    public static final int EVENT_IMAPINVALIDATION = 215;
    public static final int EVENT_IMAPBATCHINVALIDATION = 216;

    private EventMessageConst() {
    }
}
