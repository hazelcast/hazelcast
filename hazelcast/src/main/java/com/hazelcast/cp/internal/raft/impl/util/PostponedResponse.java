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

package com.hazelcast.cp.internal.raft.impl.util;

/**
 * The special response class that is used when a Raft entry does not return
 * a response at the time of its commit, and will return a response with
 * another commit in future. Therefore, when {@link PostponedResponse#INSTANCE}
 * is returned as response of a Raft commit, its future object will not be
 * completed.
 */
public final class PostponedResponse {

    public static final PostponedResponse INSTANCE = new PostponedResponse();

    private PostponedResponse() {
    }

}
