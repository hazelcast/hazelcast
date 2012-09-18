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

package com.hazelcast.map;

import com.hazelcast.util.Clock;

class ScheduledOperation {

    LockAwareOperation op;
    long timeToExpire;
    long timeout;
    boolean valid = true;

    ScheduledOperation(LockAwareOperation op) {
        this.op = op;
        setTimeout(op.getTimeout());
    }

    boolean expired() {
        return !valid || (timeout != -1 && Clock.currentTimeMillis() >= timeToExpire);
    }

    void setTimeout(long newTimeout) {
        if (newTimeout > -1) {
            this.timeout = newTimeout;
            timeToExpire = Clock.currentTimeMillis() + newTimeout;
            if (timeToExpire < 0) {
                this.timeout = -1;
                this.timeToExpire = Long.MAX_VALUE;
            }
        } else {
            this.timeout = -1;
        }
    }

    boolean isValid() {
        return valid;
    }

    public void setValid(final boolean valid) {
        this.valid = valid;
    }

    public LockAwareOperation getOperation() {
        return op;
    }
}
