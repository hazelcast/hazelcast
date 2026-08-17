/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector;

import com.hazelcast.core.HazelcastException;

import java.io.Serial;

/**
 * Exception that indicates that the state found on this index disallows
 * mutation. This may happen:
 * <ul>
 * <li>because the cleanup operation is in progress</li>
 * </ul>
 *
 * @since 5.5
 */
public class IndexMutationDisallowedException extends HazelcastException {

    @Serial
    private static final long serialVersionUID = -4007308629175331879L;

    public IndexMutationDisallowedException(String message) {
        super(message);
    }
}
