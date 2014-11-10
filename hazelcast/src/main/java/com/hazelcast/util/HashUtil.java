/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import java.util.Arrays;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"SF_SWITCH_FALLTHROUGH", "SF_SWITCH_NO_DEFAULT"})
public final class HashUtil {

    /**
     * Hash code for multiple objects using {@link Arrays#hashCode(Object[])}.
     **/
    public static int hashCode(Object ... objects) {
        return Arrays.hashCode(objects);
    }

    private HashUtil(){}
}
