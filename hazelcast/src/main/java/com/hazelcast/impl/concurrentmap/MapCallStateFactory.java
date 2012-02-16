/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.concurrentmap;

import com.hazelcast.impl.base.CallStateLog;
import com.hazelcast.impl.base.DistributedLock;

public class MapCallStateFactory {

    public static CallStateLog newScheduleRequest(DistributedLock lock, int size) {
        return new RequestScheduled(lock, size);
    }

    static class RequestScheduled extends CallStateLog {
        private final DistributedLock lock;
        private final int size;

        public RequestScheduled(DistributedLock lock, int size) {
            this.lock = lock;
            this.size = size;
        }

        @Override
        public String toString() {
            DistributedLock l = lock;
            StringBuilder sb = new StringBuilder("Scheduled[size=");
            sb.append(size).append("]");
            if (l != null) {
                sb.append(" {");
                sb.append(l.toString());
                sb.append("}");
            }
            return sb.toString();
        }
    }
}
