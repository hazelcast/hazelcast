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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.sql.impl.worker.QueryFragmentContext;

/**
 * Executor which has an upstream executor and hence delegate to it at some stages.
 */
public abstract class AbstractUpstreamAwareExec extends AbstractExec {

    protected final UpstreamState state;

    /**
     * Constructor.
     *
     * @param upstream Upstream stage.
     */
    protected AbstractUpstreamAwareExec(int id, Exec upstream) {
        super(id);

        state = new UpstreamState(upstream);
    }

    /**
     * For testing only.
     */
    public Exec getUpstream() {
        return state.getUpstream();
    }

    @Override
    protected final void setup0(QueryFragmentContext ctx) {
        state.setup(ctx);

        setup1(ctx);
    }

    protected void setup1(QueryFragmentContext ctx) {
        // No-op.
    }
}
