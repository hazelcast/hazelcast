/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
    public static final int ILLEGAL_ACCESS_EXCEPTION = 26;
    public static final int ILLEGAL_ACCESS_ERROR = 27;
    public static final int ILLEGAL_MONITOR_STATE = 28;
    public static final int ILLEGAL_STATE = 29;
    public static final int ILLEGAL_THREAD_STATE = 30;
    public static final int INDEX_OUT_OF_BOUNDS = 31;
    public static final int INTERRUPTED = 32;
    public static final int INVALID_ADDRESS = 33;
    public static final int INVALID_CONFIGURATION = 34;
    public static final int MEMBER_LEFT = 35;
    public static final int NEGATIVE_ARRAY_SIZE = 36;
    public static final int NO_SUCH_ELEMENT = 37;
    public static final int NOT_SERIALIZABLE = 38;
    public static final int NULL_POINTER = 39;
    public static final int OPERATION_TIMEOUT = 40;
    public static final int PARTITION_MIGRATING = 41;
    public static final int QUERY = 42;
    public static final int QUERY_RESULT_SIZE_EXCEEDED = 43;
    public static final int QUORUM = 44;
    public static final int REACHED_MAX_SIZE = 45;
    public static final int REJECTED_EXECUTION = 46;
    public static final int REMOTE_MAP_REDUCE = 47;
    public static final int RESPONSE_ALREADY_SENT = 48;
    public static final int RETRYABLE_HAZELCAST = 49;
    public static final int RETRYABLE_IO = 50;
    public static final int RUNTIME = 51;
    public static final int SECURITY = 52;
    public static final int SOCKET = 53;
    public static final int STALE_SEQUENCE = 54;
    public static final int TARGET_DISCONNECTED = 55;
    public static final int TARGET_NOT_MEMBER = 56;
    public static final int TIMEOUT = 57;
    public static final int TOPIC_OVERLOAD = 58;
    public static final int TOPOLOGY_CHANGED = 59;
    public static final int TRANSACTION = 60;
    public static final int TRANSACTION_NOT_ACTIVE = 61;
    public static final int TRANSACTION_TIMED_OUT = 62;
    public static final int URI_SYNTAX = 63;
    public static final int UTF_DATA_FORMAT = 64;
    public static final int UNSUPPORTED_OPERATION = 65;
    public static final int WRONG_TARGET = 66;
    public static final int XA = 67;
    public static final int ACCESS_CONTROL = 68;
    public static final int LOGIN = 69;
    public static final int UNSUPPORTED_CALLBACK = 70;
    public static final int NO_DATA_MEMBER = 71;
    public static final int REPLICATED_MAP_CANT_BE_CREATED = 72;
    public static final int MAX_MESSAGE_SIZE_EXCEEDED = 73;
    public static final int WAN_REPLICATION_QUEUE_FULL = 74;
    public static final int ASSERTION_ERROR = 75;
    public static final int OUT_OF_MEMORY_ERROR = 76;
    public static final int STACK_OVERFLOW_ERROR = 77;
    public static final int NATIVE_OUT_OF_MEMORY_ERROR = 78;
    public static final int SERVICE_NOT_FOUND = 79;
    public static final int STALE_TASK_ID = 80;
    public static final int DUPLICATE_TASK = 81;
    public static final int STALE_TASK = 82;
    public static final int LOCAL_MEMBER_RESET = 83;
    public static final int INDETERMINATE_OPERATION_STATE = 84;
    public static final int FLAKE_ID_NODE_ID_OUT_OF_RANGE_EXCEPTION = 85;
    public static final int TARGET_NOT_REPLICA_EXCEPTION = 86;
    public static final int MUTATION_DISALLOWED_EXCEPTION = 87;
    public static final int CONSISTENCY_LOST_EXCEPTION = 88;

    // These exception codes are reserved to by used by hazelcast-jet project
    public static final int JET_EXCEPTIONS_RANGE_START = 500;
    public static final int JET_EXCEPTIONS_RANGE_END = 600;

    /**
     * These codes onwards are reserved to be used by the end-user
     */
    public static final int USER_EXCEPTIONS_RANGE_START = 1000;

    private ClientProtocolErrorCodes() {
    }
}
