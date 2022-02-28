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

package com.hazelcast.jet.impl.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.CompletableFuture;

/**
 * A future which prevents completion by outside caller
 */
public class NonCompletableFuture extends CompletableFuture<Void> {

    public NonCompletableFuture() {
    }

    public NonCompletableFuture(CompletableFuture<?> chainedFuture) {
        chainedFuture.whenComplete((r, t) -> {
            if (t != null) {
                internalCompleteExceptionally(t);
            } else {
                internalComplete();
            }
        });
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        throw new UnsupportedOperationException("This future can't be completed by an outside caller");
    }

    @Override
    public boolean complete(Void value) {
        throw new UnsupportedOperationException("This future can't be completed by an outside caller");
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("This future can't be cancelled by an outside caller");
    }

    @Override
    public void obtrudeException(Throwable ex) {
        throw new UnsupportedOperationException("This future can't be completed by an outside caller");
    }

    @Override
    public void obtrudeValue(Void value) {
        throw new UnsupportedOperationException("This future can't be completed by an outside caller");
    }

    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    public void internalComplete() {
        super.complete(null);
    }

    public void internalCompleteExceptionally(Throwable ex) {
        super.completeExceptionally(ex);
    }

}
