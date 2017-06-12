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

package com.hazelcast.jet;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * A policy object that decides when when the watermark has advanced
 * enough to emit a new watermark item.
 */
public interface WatermarkEmissionPolicy extends Serializable {

    /**
     * Decides whether a watermark item with the supplied {@code currentWm}
     * value should be emitted, given the last emitted value {@code
     * lastEmittedWm}.
     */
    boolean shouldEmit(long currentWm, long lastEmittedWm);

    /**
     * Returns a policy that ensures that each emitted watermark has a higher
     * value than the last one.
     */
    @Nonnull
    static WatermarkEmissionPolicy suppressDuplicates() {
        return (currentWm, lastEmittedWm) -> currentWm > lastEmittedWm;
    }

}
