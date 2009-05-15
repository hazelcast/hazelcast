/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

public interface Constants {

    interface NodeTypes {
        public static final int NODE_MEMBER = 1;

        public static final int NODE_SUPER_CLIENT = 2;

        public static final int NODE_JAVA_CLIENT = 3;

        public static final int NODE_CSHARP_CLIENT = 4;

    }

    interface Objects {
        public static final Object OBJECT_DONE = new Object();

        public static final Object OBJECT_NULL = new Object();

        public static final Object OBJECT_CANCELLED = new Object();

        public static final Object OBJECT_LAST = new Object();

        public static final Object OBJECT_REDO = new Object();

        public static final Object OBJECT_TIMEOUT = new Object();
    }

    interface Timeouts {

        public static final long TIMEOUT_ADDITION = 10000;

        public static final long DEFAULT_LOCK_WAIT = 8000;

        public static final long DEFAULT_TXN_TIMEOUT = 8000;

        public static final long DEFAULT_TIMEOUT = 1000 * 1000;
    }

    interface MapTypes {

        public final static byte MAP_TYPE_MAP = 1;

        public final static byte MAP_TYPE_SET = 2;

        public final static byte MAP_TYPE_LIST = 3;

        public final static byte MAP_TYPE_MULTI_MAP = 4;

    }

    interface ClusterOperations {

        public static final int OP_RESPONSE = 1;

        public static final int OP_HEARTBEAT = 2;

        public static final int OP_REMOTELY_PROCESS = 3;

        public static final int OP_REMOTELY_PROCESS_AND_RESPOND = 4;

        public static final int OP_REMOTELY_BOOLEAN_CALLABLE = 5;

        public static final int OP_REMOTELY_OBJECT_CALLABLE = 6;

    }

    interface EventOperations {
        public static final int OP_LISTENER_ADD = 51;

        public static final int OP_LISTENER_REMOVE = 52;

        public static final int OP_EVENT = 53;
    }

    interface ExecutorOperations {
        public static final int OP_EXE_REMOTE_EXECUTION = 81;

        public static final int OP_STREAM = 82;
    }

    interface BlockingQueueOperations {

        public static final int OP_B_POLL = 101;

        public static final int OP_B_OFFER = 102;

        public static final int OP_B_ADD_BLOCK = 103;

        public static final int OP_B_REMOVE_BLOCK = 104;

        public static final int OP_B_FULL_BLOCK = 105;

        public static final int OP_B_BACKUP_ADD = 107;

        public static final int OP_B_BACKUP_REMOVE = 108;

        public static final int OP_B_SIZE = 109;

        public static final int OP_B_PEEK = 110;

        public static final int OP_B_READ = 111;

        public static final int OP_B_REMOVE = 112;

        public static final int OP_B_TXN_BACKUP_POLL = 113;

        public static final int OP_B_TXN_COMMIT = 114;

        public static final int OP_B_PUBLISH = 115;

        public static final int OP_B_ADD_TOPIC_LISTENER = 116;

    }

    interface ConcurrentMapOperations {

        public static final int OP_CMAP_PUT = 201;

        public static final int OP_CMAP_GET = 202;

        public static final int OP_CMAP_REMOVE = 203;

        public static final int OP_CMAP_REMOVE_ITEM = 204;

        public static final int OP_CMAP_BLOCK_INFO = 206;

        public static final int OP_CMAP_SIZE = 207;

        public static final int OP_CMAP_CONTAINS = 208;

        public static final int OP_CMAP_ITERATE = 209;

        public static final int OP_CMAP_ITERATE_KEYS = 210;

        public static final int OP_CMAP_LOCK = 212;

        public static final int OP_CMAP_UNLOCK = 213;

        public static final int OP_CMAP_BLOCKS = 214;

        public static final int OP_CMAP_MIGRATION_COMPLETE = 216;

        public static final int OP_CMAP_PUT_IF_ABSENT = 219;

        public static final int OP_CMAP_REMOVE_IF_SAME = 220;

        public static final int OP_CMAP_REPLACE_IF_NOT_NULL = 221;

        public static final int OP_CMAP_REPLACE_IF_SAME = 222;

        public static final int OP_CMAP_LOCK_RETURN_OLD = 223;

        public static final int OP_CMAP_READ = 224;

        public static final int OP_CMAP_ADD_TO_LIST = 225;

        public static final int OP_CMAP_ADD_TO_SET = 226;

        public static final int OP_CMAP_MIGRATE_RECORD = 227;

        public static final int OP_CMAP_PUT_MULTI = 228;

        public static final int OP_CMAP_REMOVE_MULTI = 229;

        public static final int OP_CMAP_VALUE_COUNT = 230;

        public static final int OP_CMAP_BACKUP_PUT = 231;

        public static final int OP_CMAP_BACKUP_REMOVE = 233;

        public static final int OP_CMAP_BACKUP_REMOVE_MULTI = 234;

        public static final int OP_CMAP_BACKUP_LOCK = 235;

        public static final int OP_CMAP_BACKUP_ADD = 236;

        public static final int OP_CMAP_EVICT = 237;


    }

    public interface ResponseTypes {

        public static final byte RESPONSE_NONE = 2;

        public static final byte RESPONSE_SUCCESS = 3;

        public static final byte RESPONSE_FAILURE = 4;

        public static final byte RESPONSE_REDO = 5;

    }

}
