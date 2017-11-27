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

import com.hazelcast.jet.function.DistributedIntFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Collection;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.stream.Collectors.toList;

/**
 * A {@link ProcessorSupplier} which closes created processor instances when
 * the job is complete. Class is useful to close external resources such as
 * connections or files.
 * <p>
 * The {@code close()} method is called after all other calls on any local
 * processor have returned.
 *
 * @param <E> Processor type
 */
public class CloseableProcessorSupplier<E extends Processor & Closeable> implements ProcessorSupplier {

    static final long serialVersionUID = 1L;

    private final DistributedIntFunction<Collection<E>> supplier;

    private transient ILogger logger;
    private transient Collection<E> processors;

    /**
     * @param simpleSupplier Supplier to create processor instances.
     */
    public CloseableProcessorSupplier(DistributedSupplier<E> simpleSupplier) {
        this(count -> IntStream.range(0, count)
                               .mapToObj(i -> simpleSupplier.get())
                               .collect(toList()));
    }

    /**
     * @param supplier Supplier to create processor instances.
     *                 Parameter is the number of processors that should be in the collection.
     */
    public CloseableProcessorSupplier(DistributedIntFunction<Collection<E>>  supplier) {
        this.supplier = supplier;
    }

    @Override
    public void init(@Nonnull Context context) {
        logger = context.logger();
    }

    @Nonnull @Override
    public Collection<E> get(int count) {
        assert processors == null;
        return processors = supplier.apply(count);
    }

    @Override
    public void complete(Throwable error) {
        if (processors == null) {
            return;
        }

        Throwable firstError = null;
        for (E p : processors) {
            try {
                p.close();
            } catch (Throwable e) {
                if (firstError == null) {
                    firstError = e;
                } else {
                    logger.severe(e);
                }
            }
        }
        if (firstError != null) {
            throw sneakyThrow(firstError);
        }
    }

}
