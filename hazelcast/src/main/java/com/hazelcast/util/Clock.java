/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

public final class Clock {
    private static final ClockImpl CLOCK;

    private Clock() {
    }

    public static long currentTimeMillis() {
        return CLOCK.currentTimeMillis();
    }

    static {
        final String clockOffset = System.getProperty("com.hazelcast.clock.offset");
        long offset = 0L;
        if (clockOffset != null) {
            try {
                offset = Long.parseLong(clockOffset);
            } catch (NumberFormatException ignore) {
            }
        }
        if (offset == 0L) {
            CLOCK = new SystemClock();
        } else {
            CLOCK = new SystemOffsetClock(offset);
        }
    }

    private abstract static class ClockImpl {

        protected abstract long currentTimeMillis();
    }

    private static final class SystemClock extends ClockImpl {

        @Override
        protected long currentTimeMillis() {
            return System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return "SystemClock";
        }
    }

    private static final class SystemOffsetClock extends ClockImpl {

        private final long offset;

        private SystemOffsetClock(final long offset) {
            this.offset = offset;
        }

        @Override
        protected long currentTimeMillis() {
            return System.currentTimeMillis() + offset;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("SystemOffsetClock");
            sb.append("{offset=").append(offset);
            sb.append('}');
            return sb.toString();
        }
    }


}
