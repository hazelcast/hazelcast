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

package com.hazelcast.jet.cdc;

import com.hazelcast.jet.annotation.EvolvingApi;

import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * Describes the nature of a CDC event, mainly the type of action performed
 * on a database record: insertion, update and deletion. There are also
 * some special values: "sync", which has the same effect as an insert, but
 * originates from the initial database snapshot (as opposed to the
 * database changelog), and "unspecified", used for a few special CDC
 * events (like heartbeats).
 *
 * @since Jet 4.2
 */
@EvolvingApi
public enum Operation {
    /**
     * {@code ChangeRecord} doesn't have an operation field, for example
     * heartbeats.
     */
    UNSPECIFIED(null),
    /**
     * Just like {@link #INSERT}, but coming from the initial DB snapshot (as
     * opposed to the change log).
     */
    SYNC('r'),
    /**
     * Record insertion, sourced from the DB changelog.
     */
    INSERT('c'),
    /**
     * Record update, sourced from the DB changelog.
     */
    UPDATE('u'),
    /**
     * Record deletion, sourced from the DB changelog.
     */
    DELETE('d');

    private final Character id;

    Operation(Character id) {
        this.id = id;
    }

    /**
     * Parses the string present in a CDC message into the corresponding
     * {@code Operation} enum member.
     * <p>
     * If the argument is {@code null}, it returns {@link #UNSPECIFIED}.
     *
     * @throws ParsingException if the input string doesn't represent an expected value
     */
    public static Operation get(@Nullable String opcode) throws ParsingException {
        Operation op = Lookup.get(opcode);
        if (op == null) {
            throw new ParsingException("'" + opcode + "' is not a valid operation id");
        }
        return op;
    }

    private static class Lookup {

        private static final Operation[] ARRAY;

        static {
            int maxId = Stream.of(values())
                    .filter(op -> op.id != null)
                    .map(op -> (int) op.id)
                    .max(Integer::compareTo)
                    .orElse(0);

            ARRAY = new Operation[maxId + 1];
            Stream.of(values())
                    .filter(op -> op.id != null)
                    .forEach(op -> ARRAY[op.id] = op);
        }

        static Operation get(String opcode) {
            if (opcode == null) {
                return UNSPECIFIED;
            }
            if (opcode.length() != 1) {
                return null;
            }
            int index = opcode.charAt(0);
            if (index >= ARRAY.length) {
                return null;
            }
            return ARRAY[index];
        }

    }
}
