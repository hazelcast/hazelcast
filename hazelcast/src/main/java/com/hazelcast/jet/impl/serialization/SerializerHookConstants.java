/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.serialization;

import com.hazelcast.internal.serialization.impl.SerializationConstants;

public final class SerializerHookConstants {

    /**
     * Start of reserved space for Jet-specific serializers.
     * Any ID greater than this number might be used by some other Hazelcast serializer.
     * For more information, {@see SerializationConstants}
     */
    public static final int JET_RESERVED_SPACE_START = SerializationConstants.JET_SERIALIZER_FIRST;

    public static final int MAP_ENTRY = -300;
    public static final int CUSTOM_CLASS_LOADED_OBJECT = -301;
    public static final int OBJECT_ARRAY = -302;

    public static final int LONG_ACC = -303;
    public static final int DOUBLE_ACC = -304;
    public static final int MUTABLE_REFERENCE = -305;
    public static final int LIN_TREND_ACC = -306;
    public static final int LONG_LONG_ACC = -307;
    public static final int LONG_DOUBLE_ACC = -308;
    public static final int PICK_ANY_ACC = -309;
    public static final int TUPLE2 = -310;
    public static final int TUPLE3 = -311;
    public static final int TUPLE4 = -312;
    public static final int TUPLE5 = -313;
    public static final int TAG = -314;
    public static final int ITEMS_BY_TAG = -315;
    public static final int WATERMARK = -316;
    public static final int SNAPSHOT_BARRIER = -317;
    public static final int DONE_ITEM = -318;
    public static final int BROADCAST_ENTRY = -319;
    public static final int BROADCAST_KEY = -320;
    public static final int WINDOW_RESULT = -321;
    public static final int KEYED_WINDOW_RESULT = -322;
    public static final int HASH_MAP = -323;
    public static final int HASH_SET = -324;
    public static final int JET_EVENT = -325;
    public static final int TIMESTAMPED_ITEM = -326;
    public static final int QUERY_ENTRY = -327;
    public static final int DEFERRED_MAP = -328;
    public static final int AVRO_UTF8 = -329;

    public static final int CDC_RECORD = -340;
    public static final int CDC_RECORD_PART = -341;
    public static final int CDC_SOURCE_STATE = -342;


    // reserved for hadoop module: -380 to -390

    /**
     * End of reserved space for Jet-specific serializers.
     * Any ID less than this number might be used by some other Hazelcast serializer.
     */
    public static final int JET_RESERVED_SPACE_END = SerializationConstants.JET_SERIALIZER_LAST;

    private SerializerHookConstants() {
    }

}
