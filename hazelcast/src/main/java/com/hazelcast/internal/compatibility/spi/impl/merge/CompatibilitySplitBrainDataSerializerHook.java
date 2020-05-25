/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.compatibility.spi.impl.merge;

import com.hazelcast.internal.compatibility.serialization.impl.CompatibilityFactoryIdHelper;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.ExpirationTimeMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;

/**
 * Data serializer hook containing (de)serialization information for communicating
 * with 3.x members over WAN.
 */
public final class CompatibilitySplitBrainDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(
            CompatibilityFactoryIdHelper.SPLIT_BRAIN_DS_FACTORY,
            CompatibilityFactoryIdHelper.SPLIT_BRAIN_DS_FACTORY_ID);

    public static final int DISCARD = 11;
    public static final int EXPIRATION_TIME = 12;
    public static final int HIGHER_HITS = 13;
    public static final int LATEST_ACCESS = 15;
    public static final int LATEST_UPDATE = 16;
    public static final int PASS_THROUGH = 17;
    public static final int PUT_IF_ABSENT = 18;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case DISCARD:
                    return new DiscardMergePolicy<>();
                case EXPIRATION_TIME:
                    return new ExpirationTimeMergePolicy<>();
                case HIGHER_HITS:
                    return new HigherHitsMergePolicy<>();
                case LATEST_ACCESS:
                    return new LatestAccessMergePolicy<>();
                case LATEST_UPDATE:
                    return new LatestUpdateMergePolicy<>();
                case PASS_THROUGH:
                    return new PassThroughMergePolicy<>();
                case PUT_IF_ABSENT:
                    return new PutIfAbsentMergePolicy<>();
                default:
                    return null;
            }
        };
    }
}
