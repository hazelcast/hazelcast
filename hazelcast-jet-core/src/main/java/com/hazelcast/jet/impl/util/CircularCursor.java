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

package com.hazelcast.jet.impl.util;

import java.util.List;

public class CircularCursor<E> extends ListCursor<E> {

    public CircularCursor(List<E> list) {
        super(list);
    }

    /**
     * Removes the current item from the underlying collection and points the cursor
     * to the previous item, wrapping around to the last item if necessary.
     */
    public void remove() {
        list.remove(index--);
        if (index < 0) {
            index = list.size() - 1;
        }
    }

    @Override
    public boolean advance() {
        if (list.isEmpty()) {
            return false;
        }
        if (!super.advance()) {
            index = 0;
        }
        return true;
    }
}
