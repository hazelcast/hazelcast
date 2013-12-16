/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce;

public final class Reducers<KeyIn, ValueIn> {

    private <ValueOut> Reducers(Reducer<KeyIn, ValueIn, ValueOut> reducer) {
    }

    public <ValueOut> Reducers<KeyIn, ValueOut> reducer(Reducer<KeyIn, ValueIn, ValueOut> reducer) {
        return (Reducers) this;
    }

    public <BaseValueIn, ValueOut> Reducer<KeyIn, BaseValueIn, ValueOut> build() {
        return null;
    }

    public static <KeyIn, ValueIn, ValueOut> Reducers<KeyIn, ValueOut> chain(Reducer<KeyIn, ValueIn, ValueOut> reducer) {
        return (Reducers) new Reducers<KeyIn, ValueIn>(reducer);
    }

}
