/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

/**
 * Used to assign a {@link STATE} to a key.
 *
 * That {@link STATE} is used when deciding whether or not a key can be puttable to a near-cache.
 * Because there is a possibility that an invalidation for a key can be received before putting that
 * key into near-cache, in that case, key should not be put into near-cache.
 */
public interface KeyStateMarker {

    boolean tryMark(Object key);

    boolean tryUnmark(Object key);

    boolean tryRemove(Object key);

    void forceUnmark(Object key);

    void init();

    enum STATE {
        UNMARKED(0),
        MARKED(1),
        REMOVED(2);

        private int state;

        STATE(int state) {
            this.state = state;
        }

        public int getState() {
            return state;
        }
    }
}
