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
 * Message type ids of responses in client protocol. They also used to bind a request to a response inside Request
 * annotation.
 * <p/>
 * Response classes are defined    {@link com.hazelcast.client.impl.protocol.template.ResponseTemplate}
 * <p/>
 * see {@link   com.hazelcast.client.impl.protocol.template.ClientMessageTemplate#membershipListener()}
 * for  a sample usage of responses in a request.
 */
public final class ResponseMessageConst {

    public static final int VOID = 100;
    public static final int BOOLEAN = 101;
    public static final int INTEGER = 102;
    public static final int LONG = 103;
    public static final int STRING = 104;
    public static final int DATA = 105;
    public static final int LIST_DATA = 106;
    public static final int MAP_INT_DATA = 107;
    public static final int MAP_DATA_DATA = 108;
    public static final int AUTHENTICATION = 109;
    public static final int PARTITIONS = 110;
    public static final int EXCEPTION = 111;
    public static final int DISTRIBUTED_OBJECT = 112;
    public static final int ENTRY_VIEW = 113;
    public static final int JOB_PROCESS_INFO = 114;
}
