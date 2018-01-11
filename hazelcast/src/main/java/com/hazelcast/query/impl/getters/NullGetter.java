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

package com.hazelcast.query.impl.getters;

/**
 * Null getter describes neutral behaviour for a {@link Getter}.
 */
public final class NullGetter extends Getter {

    /**
     * Shared singleton instance of this class.
     */
    public static final NullGetter NULL_GETTER = new NullGetter();

    private NullGetter() {
        super(null);
    }

    @Override
    Object getValue(Object obj) throws Exception {
        return null;
    }

    @Override
    Class getReturnType() {
        return null;
    }

    @Override
    boolean isCacheable() {
        return false;
    }
}
