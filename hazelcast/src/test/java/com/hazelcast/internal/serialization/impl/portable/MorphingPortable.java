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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.nio.serialization.VersionedPortable;

/**
 * Portable for version support tests
 */
public class MorphingPortable extends MorphingBasePortable implements VersionedPortable {

    public MorphingPortable(byte aByte, boolean aBoolean, char character, short aShort, int integer,
                            long aLong, float aFloat, double aDouble, String aString) {
        super(aByte, aBoolean, character, aShort, integer, aLong, aFloat, aDouble, aString);
    }

    public MorphingPortable() {
    }

    @Override
    public int getClassVersion() {
        return 2;
    }
}
