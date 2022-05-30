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

package com.hazelcast.tpc.engine.frame;

/**
 * A {@link FrameAllocator} that doesn't do any pooling of requests.
 */
public final class UnpooledFrameAllocator implements FrameAllocator {

    public UnpooledFrameAllocator() {
    }

    @Override
    public Frame allocate() {
        throw new RuntimeException();
    }

    @Override
    public Frame allocate(int minSize) {
        return new Frame(minSize);
    }

    @Override
    public void free(Frame frame) {
    }
}
