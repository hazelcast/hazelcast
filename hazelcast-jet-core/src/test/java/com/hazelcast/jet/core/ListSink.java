/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

class ListSink extends AbstractProcessor {

    private final List<Object> list = new ArrayList<>();

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        list.add(item);
        return true;
    }

    List<Object> getList() {
        return list;
    }
}
