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

package com.hazelcast.cp.internal;

import com.hazelcast.cluster.Address;

/**
 * An abstraction to implement blocking Raft ops. A blocking Raft op requires
 * caller information to be provided before the Raft op is being replicated
 * and committed by the Raft consensus algorithm. The Raft invocation mechanism
 * provides this functionality. Once the Raft op is committed, it is reported
 * as a live operation to the Hazelcast's invocation system, even if the total
 * wait duration is longer then the configured operation timeout duration.
 * A blocking Raft op can fail with operation timeout if it is not committed
 * before the configured operation timeout duration passes.
 */
public interface CallerAware {

    /**
     * Sets the caller information on the Raft op
     */
    void setCaller(Address callerAddress, long callId);

}
