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

package com.hazelcast.jet.pipeline;

import com.hazelcast.internal.serialization.SerializableByConvention;

/**
 * Represents the definition of a session window.
 *
 * @since Jet 3.0
 */
@SerializableByConvention
public class SessionWindowDefinition extends WindowDefinition {

    private static final long serialVersionUID = 1L;

    private final long sessionTimeout;

    SessionWindowDefinition(long sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    public SessionWindowDefinition setEarlyResultsPeriod(long earlyResultPeriodMs) {
        return (SessionWindowDefinition) super.setEarlyResultsPeriod(earlyResultPeriodMs);
    }

    /**
     * Returns the session timeout, which is the largest difference in the
     * timestamps of any two consecutive events in the session window.
     */
    public long sessionTimeout() {
        return sessionTimeout;
    }
}
