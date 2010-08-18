/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.nio.Data;

public interface Constants {

    interface Objects {
        public static final Object OBJECT_DONE = new Object();

        public static final Object OBJECT_NULL = new Object();

        public static final Object OBJECT_NO_RESPONSE = new Object();

        public static final Object OBJECT_CANCELLED = new Object();

        public static final Object OBJECT_MEMBER_LEFT = new Object();

        public static final Object OBJECT_REDO = new Object();
    }

    interface IO {

        public static final int KILO_BYTE = 1024;

        public static final Data EMPTY_DATA = new Data();
    }

    interface Timeouts {

        public static final long DEFAULT_TXN_TIMEOUT = 8000;

        public static final long DEFAULT_TIMEOUT = 1000 * 1000;
    }

    public interface ResponseTypes {

        public static final byte RESPONSE_NONE = 2;

        public static final byte RESPONSE_SUCCESS = 3;

        public static final byte RESPONSE_FAILURE = 4;

        public static final byte RESPONSE_REDO = 5;
    }
}
