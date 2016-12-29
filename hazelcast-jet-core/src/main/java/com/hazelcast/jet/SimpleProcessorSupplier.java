/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Serializable variant of the standard {@code Supplier<Processor>}. A convenience
 * over the full {@link ProcessorSupplier} which abstracts away the boilerplate
 * of producing the required number of processor instances, when each created
 * instance will be the same.
 * <p>
 * <strong>NOTE:</strong> this type should not be abused with a stateful
 * implementation which produces a different processor each time. In such a
 * case the full {@code ProcessorSupplier} type should be implemented.
 */
@FunctionalInterface
public interface SimpleProcessorSupplier extends Serializable, Supplier<Processor> {
}
