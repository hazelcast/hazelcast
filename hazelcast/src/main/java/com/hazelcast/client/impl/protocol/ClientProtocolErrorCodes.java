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
 * Each exception that are defined in client protocol have unique identifier which are error code.
 * All error codes defined in protocol are listed in this class.
 */
public final class ClientProtocolErrorCodes {

    public static final int UNDEFINED = 0;
    public static final int ARRAY_INDEX_OUT_OF_BOUNDS = 1;
    public static final int ARRAY_STORE = 2;
    public static final int AUTHENTICATION = 3;
    public static final int CACHE = 4;
    public static final int CACHE_LOADER = 5;
    public static final int CACHE_NOT_EXISTS = 6;
    public static final int CACHE_WRITER = 7;
    public static final int CALLER_NOT_MEMBER = 8;
    public static final int CANCELLATION = 9;
    public static final int CLASS_CAST = 10;
    public static final int CLASS_NOT_FOUND = 11;
    public static final int CONCURRENT_MODIFICATION = 12;
    public static final int CONFIG_MISMATCH = 13;
    public static final int CONFIGURATION = 14;
    public static final int DISTRIBUTED_OBJECT_DESTROYED = 15;
    public static final int DUPLICATE_INSTANCE_NAME = 16;
    public static final int EOF = 17;
    public static final int ENTRY_PROCESSOR = 18;
    public static final int EXECUTION = 19;
    public static final int HAZELCAST = 20;
    public static final int HAZELCAST_INSTANCE_NOT_ACTIVE = 21;
    public static final int HAZELCAST_OVERLOAD = 22;
    public static final int HAZELCAST_SERIALIZATION = 23;
    public static final int IO = 24;
    public static final int ILLEGAL_ARGUMENT = 25;
    public static final int ILLEGAL_MONITOR_STATE = 26;
    public static final int ILLEGAL_STATE = 27;
    public static final int ILLEGAL_THREAD_STATE = 28;
    public static final int INDEX_OUT_OF_BOUNDS = 29;
    public static final int INTERRUPTED = 30;
    public static final int INVALID_ADDRESS = 31;
    public static final int INVALID_CONFIGURATION = 32;
    public static final int MEMBER_LEFT = 33;
    public static final int NEGATIVE_ARRAY_SIZE = 34;
    public static final int NO_SUCH_ELEMENT = 35;
    public static final int NOT_SERIALIZABLE = 36;
    public static final int NULL_POINTER = 37;
    public static final int OPERATION_TIMEOUT = 38;
    public static final int PARTITION_MIGRATING = 39;
    public static final int QUERY = 40;
    public static final int QUERY_RESULT_SIZE_EXCEEDED = 41;
    public static final int QUORUM = 42;
    public static final int REACHED_MAX_SIZE = 43;
    public static final int REJECTED_EXECUTION = 44;
    public static final int REMOTE_MAP_REDUCE = 45;
    public static final int RESPONSE_ALREADY_SENT = 46;
    public static final int RETRYABLE_HAZELCAST = 47;
    public static final int RETRYABLE_IO = 48;
    public static final int RUNTIME = 49;
    public static final int SECURITY = 50;
    public static final int SOCKET = 51;
    public static final int STALE_SEQUENCE = 52;
    public static final int TARGET_DISCONNECTED = 53;
    public static final int TARGET_NOT_MEMBER = 54;
    public static final int TIMEOUT = 55;
    public static final int TOPIC_OVERLOAD = 56;
    public static final int TOPOLOGY_CHANGED = 57;
    public static final int TRANSACTION = 58;
    public static final int TRANSACTION_NOT_ACTIVE = 59;
    public static final int TRANSACTION_TIMED_OUT = 60;
    public static final int URI_SYNTAX = 61;
    public static final int UTF_DATA_FORMAT = 62;
    public static final int UNSUPPORTED_OPERATION = 63;
    public static final int WRONG_TARGET = 64;
    public static final int XA = 65;
    public static final int ACCESS_CONTROL = 66;
    public static final int LOGIN = 67;
    public static final int UNSUPPORTED_CALLBACK = 68;

    private ClientProtocolErrorCodes() {

    }
}
