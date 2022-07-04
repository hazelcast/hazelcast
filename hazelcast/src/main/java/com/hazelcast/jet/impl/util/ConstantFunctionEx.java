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

package com.hazelcast.jet.impl.util;

import com.hazelcast.function.FunctionEx;

/**
 * A function returning a constant.
 * <p>
 * This is a trivial lambda, but we want to be able to do instanceof check on
 * it to be able to do some optimizations.
 */
public class ConstantFunctionEx<T, R> implements FunctionEx<T, R> {

    private static final long serialVersionUID = 1L;

    private final R key;

    public ConstantFunctionEx(R key) {
        this.key = key;
    }

    @Override
    public R applyEx(T t) {
        return key;
    }
}
