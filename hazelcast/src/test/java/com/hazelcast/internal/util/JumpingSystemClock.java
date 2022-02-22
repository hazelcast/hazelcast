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

package com.hazelcast.internal.util;

import java.util.concurrent.TimeUnit;

class JumpingSystemClock extends Clock.ClockImpl {

    static final String JUMP_AFTER_SECONDS_PROPERTY = "com.hazelcast.clock.jump.after";

    private final long jumpAfter;
    private final long jumpOffset;

    JumpingSystemClock() {
        String clockOffset = System.getProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET);
        String jumpAfterSeconds = System.getProperty(JUMP_AFTER_SECONDS_PROPERTY);
        try {
            jumpOffset = Long.parseLong(clockOffset);
            jumpAfter = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(Integer.parseInt(jumpAfterSeconds));
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected long currentTimeMillis() {
        long now = System.currentTimeMillis();
        if (now >= jumpAfter) {
            now += jumpOffset;
        }
        return now;
    }
}
