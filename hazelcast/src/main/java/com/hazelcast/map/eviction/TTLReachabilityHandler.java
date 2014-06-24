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
 * TTLReachabilityHandler.
 */
class TTLReachabilityHandler extends AbstractReachabilityHandler {

    public TTLReachabilityHandler() {
    }

    @Override
    public Record handle(Record record, long criteria, long time) {
        if (record == null) {
            return null;
        }
        boolean result;
        final long ttl = record.getTtl();
        // when ttl is zero or negative, it should remain eternally.
        if (ttl < 1L) {
            return record;
        }
        final long creationTime = record.getCreationTime();

        assert ttl > 0L : String.format("wrong ttl %d", ttl);
        assert creationTime > 0L : String.format("wrong creationTime %d", creationTime);
        assert time > 0L : String.format("wrong time %d", time);
        assert time >= creationTime : String.format("time >= lastUpdateTime (%d >= %d)",
                time, creationTime);
        result = time - creationTime >= ttl;
        return result ? null : record;
    }

    @Override
    public short niceNumber() {
        return 0;
    }

}
