/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt.physical.index;

import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import org.apache.calcite.rex.RexNode;

public class IndexFilterDescriptor {

    private final IndexFilter indexFilter;
    private final RexNode indexExp;
    private final RexNode remainderExp;

    public IndexFilterDescriptor(IndexFilter indexFilter, RexNode indexExp, RexNode remainderExp) {
        this.indexFilter = indexFilter;
        this.indexExp = indexExp;
        this.remainderExp = remainderExp;
    }

    public IndexFilter getIndexFilter() {
        return indexFilter;
    }

    public RexNode getIndexExp() {
        return indexExp;
    }

    public RexNode getRemainderExp() {
        return remainderExp;
    }
}
