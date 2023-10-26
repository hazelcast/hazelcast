/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.iterator;

import java.util.Iterator;

/**
 * The iterator that also extends the {@link AutoCloseable} interface.
 * <p>
 * NOTE: It is up to the implementation if {@link #close()} is always required to be called,
 * or only if the iterator hasn't been exhausted.
 *
 * @param <E> see {@link Iterator}.E.
 */
public interface AutoCloseableIterator<E> extends Iterator<E>, AutoCloseable {
}
