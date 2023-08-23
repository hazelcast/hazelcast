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

package com.hazelcast.internal.tpcengine.util;

import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.sneakyThrow;

/**
 * <p/>
 * A builder contains the parameters injected into some object. Using the
 * builder the object can be configured. Effectively it a fancy constructor.
 * <p>
 * A builder should only be used once to create an object.
 * <p/>
 * The idea is that each builder can have zero or more parameters (probably
 * you want to make them public fields) and on completion, call {@link #build()}
 * method. This will first call {@link #conclude()} to initialize and or validate
 * the fields of the builder and on success, the {@link #construct()} is called.
 * <p/>
 * Advantages of using the builder:
 * <ol>
 *     <li>it is easier to decouple objects because they can exchange the
 *     parameters easily. Without a builder, if a created object needs to
 *     have one or more dependencies from a different object you either need
 *     to expose unwanted getters or make very fat constructors.
 *     </li>
 *     <li>
 *          Testing is a lot easier since you can easily control
 *          the dependencies.
 *      </li>
 *      <li>
 *          Customization of dependencies is a lot easier.
 *      </li>
 *      <li>
 *          Complex dependency setup can be localized to the builder. This makes
 *          the object that receive and send this builder a lot simpler.
 *      </li>
 *      <li>
 *          Makes it easy for all 'dependency' fields of the object to be final.
 *      </li>>
 *      <li>
 *          The constructors become very simple because the setup of the fields
 *          of an object is mostly moved to the builder.
 *      </li>
 * </ol>
 * <p/>
 * Instances of the AbstractBuilder are not threadsafe.
 */
public abstract class AbstractBuilder<E> implements AutoCloseable {

    private boolean built;

    /**
     * Finalizes the builder configuration.
     * <p/>
     * The conclude method is responsible for validating the parameters and
     * initializing fields where needed.
     * <p/>
     * This method will called exactly once per builder instance.
     */
    protected void conclude() {
    }

    /**
     * Constructs the actual object.
     * <p/>
     * The {@link #conclude()} is guaranteed to have run before this method is
     * called.
     *
     * @return the created object.
     */
    protected abstract E construct();

    /**
     * Is called automatically when either the {@link #conclude()} or {@link #construct()}
     * fails and gives the chance to cleanup resources like sockets etc.
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
    }

    /**
     * Creates the object.
     * <p/>
     * Should only be called once.
     *
     * @return the created object.
     * @throws IllegalStateException if the build method has been called before
     *                               or if there are other validation problems.
     */
    public final E build() {
        if (built) {
            throw new IllegalStateException(this + " is already built.");
        }
        built = true;

        try {
            conclude();
            return construct();
        } catch (Throwable t) {
            closeQuietly(this);
            throw sneakyThrow(t);
        }
    }
}
