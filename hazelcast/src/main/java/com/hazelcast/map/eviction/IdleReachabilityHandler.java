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

package com.hazelcast.map.eviction;

import com.hazelcast.map.record.Record;

/**
 *  IdleReachabilityHandler.
 */
class IdleReachabilityHandler extends AbstractReachabilityHandler {

    private final long idleTimeOutInNanos;

    public IdleReachabilityHandler(long idleTimeOutInNanos) {
        assert idleTimeOutInNanos > 0L : String.format("Not valid idleTimeOutInNanos %d", idleTimeOutInNanos);

        this.idleTimeOutInNanos = idleTimeOutInNanos;
    }

    @Override
    public Record handle(Record record, long criteria, long timeInNanos) {
        if (record == null) {
            return null;
        }
        boolean result;
        // lastAccessTime : updates on every touch (put/get).
        final long lastAccessTime = record.getLastAccessTime();

        assert lastAccessTime > 0L;
        assert timeInNanos > 0L;
        assert timeInNanos >= lastAccessTime;

        result = timeInNanos - lastAccessTime >= idleTimeOutInNanos;

        log("%s is asked for check and said %s", this.getClass().getName(), result);

        return result ? null : record;
    }


    @Override
    public short niceNumber() {
        return 1;
    }

}
