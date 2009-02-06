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

	interface InvocationParameters {

		public static final int MAGIC_NUMBER = 489311320;
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
	}

	interface ClusterOperations {

		public static final int OP_RESPONSE = 15;

		public static final int OP_HEARTBEAT = 18;

		public static final int OP_REMOTELY_PROCESS = 19;

		public static final int OP_REMOTELY_PROCESS_AND_RESPOND = 20;

		public static final int OP_REMOTELY_BOOLEAN_CALLABLE = 21;

		public static final int OP_REMOTELY_OBJECT_CALLABLE = 22;

	}

	interface EventOperations {
		public static final int OP_LISTENER_ADD = 201;

		public static final int OP_LISTENER_REMOVE = 202;

		public static final int OP_EVENT = 203;
	}

	interface ExecutorOperations {
		public static final int OP_EXE_REMOTE_EXECUTION = 350;

		public static final int OP_STREAM = 351;
	}

	interface BlockingQueueOperations {

		public static final int OP_B_POLL = 401;

		public static final int OP_B_OFFER = 402;

		public static final int OP_B_ADD_BLOCK = 403;

		public static final int OP_B_REMOVE_BLOCK = 404;

		public static final int OP_B_FULL_BLOCK = 405;

		public static final int OP_B_GO_NEXT = 406;

		public static final int OP_B_BACKUP_ADD = 407;

		public static final int OP_B_BACKUP_REMOVE = 408;

		public static final int OP_B_SIZE = 409;

		public static final int OP_B_PEEK = 410;

		public static final int OP_B_READ = 411;

		public static final int OP_B_REMOVE = 412;

		public static final int OP_B_TXN_BACKUP_POLL = 413;

		public static final int OP_B_TXN_COMMIT = 414;

		public static final int OP_B_PUBLISH = 415;

		public static final int OP_B_ADD_TOPIC_LISTENER = 416;

	}

	interface ConcurrentMapOperations {

		public static final int OP_CMAP_PUT = 501;

		public static final int OP_CMAP_GET = 502;

		public static final int OP_CMAP_REMOVE = 503;

		public static final int OP_CMAP_BACKUP_ADD = 504;

		public static final int OP_CMAP_BACKUP_REMOVE = 505;

		public static final int OP_CMAP_BLOCK_INFO = 506;

		public static final int OP_CMAP_REMOVE_OWNER = 507;

		public static final int OP_CMAP_SIZE = 508;

		public static final int OP_CMAP_CONTAINS_KEY = 509;

		public static final int OP_CMAP_CONTAINS_VALUE = 510;

		public static final int OP_CMAP_CLEAR = 511;

		public static final int OP_CMAP_LOCK = 512;

		public static final int OP_CMAP_UNLOCK = 513;

		public static final int OP_CMAP_BLOCKS = 514;

		// public static final int OP_CMAP_OWN_KEY = 515;

		public static final int OP_CMAP_MIGRATION_COMPLETE = 516;

		public static final int OP_CMAP_BACKUP_LOCK = 517;

		public static final int OP_CMAP_BACKUP_REMOVE_BLOCK = 518;

		public static final int OP_CMAP_PUT_IF_ABSENT = 519;

		public static final int OP_CMAP_REMOVE_IF_SAME = 520;

		public static final int OP_CMAP_REPLACE_IF_NOT_NULL = 521;

		public static final int OP_CMAP_REPLACE_IF_SAME = 522;

		public static final int OP_CMAP_LOCK_RETURN_OLD = 523;

		public static final int OP_CMAP_READ = 524;

		public static final int OP_CMAP_ADD_TO_LIST = 525;

		public static final int OP_CMAP_ADD_TO_SET = 526;

		public static final int OP_CMAP_MIGRATE_RECORD = 527;

	}

	public interface ResponseTypes {

		public static final byte RESPONSE_NONE = 2;

		public static final byte RESPONSE_SUCCESS = 3;

		public static final byte RESPONSE_FAILURE = 4;

		public static final byte RESPONSE_REDO = 5;

	}

}
