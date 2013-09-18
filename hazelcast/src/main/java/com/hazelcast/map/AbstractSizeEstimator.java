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

package com.hazelcast.map;


/**
 * Created with IntelliJ IDEA.
 * User: ahmet
 * Date: 05.09.2013
 * Time: 14:36
 */
public abstract class AbstractSizeEstimator implements SizeEstimator {

    private long _size;

    protected AbstractSizeEstimator() {
        _size = 0L;
    }

    @Override
    public long getSize() {
        return _size;
    }

    public void add(long size) {
        _size += size;
    }


    public void reset() {
        _size = 0;
    }

}
