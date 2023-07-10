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

package com.hazelcast.internal.tpcengine;

/**
 * When a task runs for a too long period on the reactor, it stalls the reactor and
 * then the {@link #onStall(Reactor, Runnable, long, long)} is called. For the time
 * being it can primarily be used for logging purposes since the handler can't influence
 * the Eventloop.
 * <p/>
 * In the future we'll probably want a stall detector that periodically checks the eventloops
 * to see if there is a task that hasn't completed but has been running for too long. When
 * this is detected, stack traces for example can be dumped. This will help a lot with
 * tracking down performance problems. Similar to the slow operation detector.
 */
public interface ReactorStallHandler {

    void onStall(Reactor reactor, TaskQueue taskQueue, Runnable cmd,
                 long startNanos, long durationNanos);
}
