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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * The {@link SkipIndexPredicate} is a predicate that prevents
 * an index from being used on the wrapped predicate.
 * <p>
 * It isn't exposed as user API; it will only be created when
 * making use of index suppression option in the
 * {@link SqlPredicate}
 * <p>
 * SkipIndexPredicate also isn't send over the wire; so we don't
 * need to worry about backward compatibility in future releases.
 */
public class SkipIndexPredicate implements Predicate {

    private Predicate target;

    public SkipIndexPredicate(Predicate target) {
        this.target = target;
    }

    public Predicate getTarget() {
        return target;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean apply(Map.Entry mapEntry) {
        return target.apply(mapEntry);
    }

    @Override
    public String toString() {
        return "SkipIndex(" + target + ')';
    }

    private void writeObject(ObjectOutputStream stream) throws IOException {
        throw new UnsupportedOperationException("can't be serialized");
    }

}
