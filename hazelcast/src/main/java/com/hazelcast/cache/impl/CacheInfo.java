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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheStorageType;
import com.hazelcast.util.StringUtil;

public class CacheInfo {

    private String name;
    private CacheStorageType cacheStorageType;

    public CacheInfo() {

    }

    public CacheInfo(String name) {
        this.name = name;
    }

    public CacheInfo(String name, CacheStorageType cacheStorageType) {
        this.name = name;
        this.cacheStorageType = cacheStorageType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public CacheStorageType getCacheStorageType() {
        return cacheStorageType;
    }

    public void setCacheStorageType(CacheStorageType cacheStorageType) {
        this.cacheStorageType = cacheStorageType;
    }

    @Override
    public int hashCode() {
        if (StringUtil.isNullOrEmpty(name)) {
            return 0;
        } else {
            return name.hashCode();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CacheInfo) {
            CacheInfo ci = (CacheInfo) obj;
            return compareNames(name, ci.name);
        } else if (obj instanceof String) {
            String str = (String) obj;
            return compareNames(name, str);
        } else {
            return false;
        }
    }

    private boolean compareNames(String name1, String name2) {
        boolean empty1 = StringUtil.isNullOrEmpty(name1);
        boolean empty2 = StringUtil.isNullOrEmpty(name2);
        if (empty1 == empty2) {
            if (empty1) {
                return true;
            } else {
                return name.equals(name2);
            }
        } else {
            return false;
        }
    }

}
