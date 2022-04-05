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

package com.hazelcast.test.jitter;

/**
 * Operational Modes for {@link JitterRule}.
 */
public enum Mode {

    /**
     * JitterRule is explicitly disabled. No jitter monitoring is performed no matter
     * what test or environment is used.
     */
    DISABLED,

    /**
     * JitterRule is explicitly enabled. Failed test will report environment hiccups (if there were any)
     */
    ENABLED,

    /**
     * JitterRule is enabled when running on Jenkins only.
     */
    JENKINS
}
