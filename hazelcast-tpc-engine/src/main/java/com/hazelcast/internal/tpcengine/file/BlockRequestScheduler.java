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

package com.hazelcast.internal.tpcengine.file;

public interface BlockRequestScheduler {

    /**
     * Reserves a single IO. The IO is not submitted to io_uring yet.
     * <p/>
     * If a non null value is returned, it is guaranteed that {@link #submit(BlockRequest)} will complete successfully.
     *
     * @return the reserved IO or null if there is no space.
     */
   BlockRequest reserve();

   void submit(BlockRequest req);
}
