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

package com.hazelcast.query.impl.getters;

/**
 * NullMultiValue getter describes neutral behaviour for a {@link Getter}.
 * Always returns a MultiResult that indicates that the MultiValue object (Collection/Array) was null.
 */
public final class NullMultiValueGetter extends Getter {

    /**
     * Shared singleton instance of this class.
     */
    public static final NullMultiValueGetter NULL_MULTIVALUE_GETTER = new NullMultiValueGetter();

    private static final MultiResult NULL_MULTIVALUE_RESULT;

    static {
        MultiResult<Object> result = new MultiResult<Object>();
        result.addNullOrEmptyTarget();
        NULL_MULTIVALUE_RESULT = new ImmutableMultiResult<Object>(result);
    }

    private NullMultiValueGetter() {
        super(null);
    }

    @Override
    Object getValue(Object obj) throws Exception {
        return NULL_MULTIVALUE_RESULT;
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
