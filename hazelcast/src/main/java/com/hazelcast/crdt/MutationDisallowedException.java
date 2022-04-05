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

package com.hazelcast.crdt;

import com.hazelcast.core.HazelcastException;

/**
 * Exception that indicates that the state found on this replica disallows
 * mutation. This may happen:
 * <ul>
 * <li>because the found state has already been migrated and is in the
 * process of cleaning up</li>
 * <li>a target replica is shutting down and in the process of replicating
 * unreplicated CRDT state</li>
 * <li>there are no data members to store the state</li>
 * </ul>
 *
 * @since 3.10
 */
public class MutationDisallowedException extends HazelcastException {
    public MutationDisallowedException(String message) {
        super(message);
    }
}
