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

package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class CompositeRemoteCallHook<K, V> implements RemoteCallHook<K, V> {

    private final List<RemoteCallHook> hooks = new ArrayList<>();

    public void add(RemoteCallHook newHook) {
        hooks.add(newHook);
    }

    @Override
    public void beforeRemoteCall(K key, Data keyData,
                                 @Nullable V value, @Nullable Data valueData) {
        for (int i = 0; i < hooks.size(); i++) {
            hooks.get(i).beforeRemoteCall(key, keyData, value, valueData);
        }
    }

    @Override
    public void onRemoteCallSuccess(@Nullable Operation remoteCall) {
        for (int i = 0; i < hooks.size(); i++) {
            hooks.get(i).onRemoteCallSuccess(remoteCall);
        }
    }

    @Override
    public void onRemoteCallFailure() {
        for (int i = 0; i < hooks.size(); i++) {
            hooks.get(i).onRemoteCallFailure();
        }
    }
}
