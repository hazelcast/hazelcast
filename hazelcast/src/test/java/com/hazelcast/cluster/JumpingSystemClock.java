/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 18/12/14
 */
class JumpingSystemClock extends Clock.ClockImpl {
    public static final int JUMP_AFTER_SECONDS = 30;

    private final long jumpAfter = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(JUMP_AFTER_SECONDS);
    private final long jumpOffset;

    public JumpingSystemClock() {
        String clockOffset = System.getProperty(Clock.HAZELCAST_CLOCK_OFFSET);
        try {
            this.jumpOffset = Long.parseLong(clockOffset);
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
