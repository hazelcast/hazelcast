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

/**
 *
 * <p/>
 * A builder contains the parameters injected into some object. Using the
 * builder the object can be configured. Effectively it a fancy constructor.
 * <p>
 * A builder should only be used once to create an object.
 * <p/>
 * The idea is that each builder can have zero or more parameters (probably
 * you want to make them public fields) and on completion, call the
 * {@link #conclude()} functionality where the parameters are validated and
 * missing values, where appropriate, are initialized to sensible defaults.
 * <p/>
 * Advantages of using the builder:
 * <ol>
 *     <li>it is easier to decouple objects because they can exchange the
 *     parameters easily. Without a builder, if a created object needs to
 *     have one or more dependencies from a different object you either need
 *     to expose unwanted getters or make very fat constructors.
 *     </li>
 *     <li>testing is a lot easier since you can easily control
 *      the dependencies.
 *      </li>
 *      <li>Customization of dependencies is a lot easier. </li>
 *      <li>
 *          Complex dependency setup can be localized to the builder. This makes
 *          the object that receive and send this builder a lot simpler.
 *      </li>
 *      <li>
 *          Makes it easy for all 'dependency' fields of the object to be final.
 *      </li>>
 * </ol>
 */
public abstract class AbstractBuilder<E> {

    private boolean concluded;
    private boolean built;

    /**
     * Finalizes the context configuration.
     * <p/>
     * The conclude method is responsible for validating the parameters and
     * initializing
     */
    protected void conclude() {
    }

    /**
     * Builds the actual object.
     *
     * @return the created object.
     */
    protected abstract E doBuild();

    protected void prebuild() {
    }

    /**
     * Creates the object.
     *
     * @param <E> the type of the object created.
     * @return the created object.
     */
    public final E build() {
        if (concluded) {
            throw new IllegalStateException(this + " is already concluded.");
        }
        concluded = true;
        conclude();
        if (built) {
            throw new IllegalStateException(this + " is already built.");
        }
        built = true;
        prebuild();
        return doBuild();
    }
}
