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

package com.hazelcast.internal.usercodedeployment.impl.filter;

import com.hazelcast.internal.util.filter.Filter;
import com.hazelcast.internal.util.collection.ArrayUtils;

/**
 * All classes match unless they are in the explicit blacklist
 */
public class ClassBlacklistFilter implements Filter<String> {

    private final String[] blacklist;

    public ClassBlacklistFilter(String... blacklisted) {
        blacklist = ArrayUtils.createCopy(blacklisted);
    }

    @Override
    public boolean accept(String className) {
        for (String blacklisted : blacklist) {
            if (className.startsWith(blacklisted)) {
                return false;
            }
        }
        return true;
    }
}
