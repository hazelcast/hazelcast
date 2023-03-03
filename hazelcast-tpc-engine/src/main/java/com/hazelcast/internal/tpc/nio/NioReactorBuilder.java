/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.Reactor;
import com.hazelcast.internal.tpc.ReactorBuilder;
import com.hazelcast.internal.tpc.ReactorType;

/**
 * A builder for a {@link NioReactor}.
 */
public class NioReactorBuilder extends ReactorBuilder {

    public NioReactorBuilder() {
        super(ReactorType.NIO);
    }

    @Override
    public Reactor build() {
        return new NioReactor(this);
    }
}
