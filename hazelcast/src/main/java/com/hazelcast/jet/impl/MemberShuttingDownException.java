/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.core.HazelcastException;

/**
 * An exception thrown when a light job or sql query is submitted to a
 * member that is in the process of shutting down. Such member is still
 * fully functional, but waits for current jobs to complete and then it
 * will shut down. The caller should retry with a different target.
 * <p>
 * This exception isn't used for non-light jobs - they can be submitted
 * even to shutting-down members, but they will not start, but just be
 * stored in the metadata so that the new master will start them.
 */
public class MemberShuttingDownException extends HazelcastException {

    public MemberShuttingDownException() {
        super("The member is shutting down, cannot submit jobs");
    }
}
