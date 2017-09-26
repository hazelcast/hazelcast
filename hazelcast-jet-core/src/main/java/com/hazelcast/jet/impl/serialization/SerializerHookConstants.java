/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
    public static final int ARRAY_LIST = -301;
    public static final int CUSTOM_CLASS_LOADED_OBJECT = -302;
    public static final int OBJECT_ARRAY = -303;
    public static final int TIMESTAMPED_ENTRY = -304;
    public static final int LONG_ACC = -305;
    public static final int DOUBLE_ACC = -306;
    public static final int MUTABLE_REFERENCE = -307;
    public static final int LIN_TREND_ACC = -308;
    public static final int LONG_LONG_ACC = -309;
    public static final int LONG_DOUBLE_ACC = -310;
    public static final int TUPLE2 = -311;
    public static final int TUPLE3 = -312;
    public static final int TWO_BAGS = -313;
    public static final int THREE_BAGS = -314;
    public static final int TAG = -315;
    public static final int ITEMS_BY_TAG = -316;
    public static final int BAGS_BY_TAG = -317;
    public static final int WATERMARK = -318;
    public static final int SNAPSHOT_BARRIER = -319;
    public static final int BROADCAST_ENTRY = -320;
    public static final int BROADCAST_KEY_REFERENCE = -321;



    // reserved for hadoop module: -380 to -390

    /**
     * End of reserved space for Jet-specific serializers.
     * Any ID less than this number might be used by some other Hazelcast serializer.
     */
    public static final int JET_RESERVED_SPACE_END = SerializationConstants.JET_SERIALIZER_LAST;

    private SerializerHookConstants() {
    }

}
