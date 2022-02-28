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

package com.hazelcast.json.internal;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;

import java.util.ArrayList;
import java.util.List;

public class TestJsonValues {

    public static final List<JsonValue> LIST = new ArrayList<JsonValue>();

    static {
        // arrays
        LIST.add(Json.array(new int[]{1, 2, 3, 4, 5}));
        LIST.add(Json.array()
                .add(Json.object())
                .add(1)
                .add(2));
        LIST.add(Json.array()
                .add(Json.object().add("a", true))
                .add(1)
                .add(2));
        LIST.add(Json.array()
                .add(1)
                .add(Json.object().add("a", true))
                .add(2));
        LIST.add(Json.array()
                .add(1)
                .add(2)
                .add(Json.object().add("a", true)));
        LIST.add(Json.array()
                .add(Json.object()
                        .add("a", true)
                        .add("b", Json.NULL))
                .add(1)
                .add(2));
        LIST.add(Json.array()
                .add(Json.array(new int[]{1, 2, 3, 4})
                        .add(Json.array(new int[]{1, 2, 3, 4})
                                .add(Json.array(new int[]{1, 2, 3, 4})
                                        .add(Json.array(new int[]{1, 2, 3, 4}))))));
        LIST.add(Json.array()
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103})));
        LIST.add(Json.array()
                .add(Json.array())
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103})));
        LIST.add(Json.array()
                .add(Json.array(new int[]{101}))
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103})));
        LIST.add(Json.array()
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array())
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103})));
        LIST.add(Json.array()
                .add(Json.TRUE)
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103}))
                .add(Json.array(new int[]{101, 102, 103})));

        // object
        LIST.add(Json.object());
        LIST.add(Json.object().add("a", 2));
        LIST.add(Json.object().add("a", 2).add("b", 34));
        LIST.add(Json.object()
                .add("a", Json.object()
                        .add("b", Json.object()
                                .add("b", Json.object()
                                        .add("b", Json.object()
                                                .add("b", 34))))));
        LIST.add(Json.object().add("arr", Json.array(new int[]{1, 2, 3, 4, 5})));
        LIST.add(Json.object()
                .add("arr6", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr5", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr4", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr3", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr2", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr1", Json.array(new int[]{1, 2, 3, 4, 5})));
        LIST.add(Json.object()
                .add("arr6", Json.array(new int[]{1, 5}))
                .add("arr5", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr4", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr3", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr2", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr1", Json.array(new int[]{1, 2, 3, 4, 5})));
        LIST.add(Json.object()
                .add("arr6", Json.array())
                .add("arr5", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr4", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr3", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr2", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr1", Json.array(new int[]{1, 2, 3, 4, 5})));
        LIST.add(Json.object()
                .add("arr6", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr5", Json.array(new int[]{1, 5}))
                .add("arr4", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr3", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr2", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr1", Json.array(new int[]{1, 2, 3, 4, 5})));
        LIST.add(Json.object()
                .add("arr6", Json.array(new int[]{34}))
                .add("arr5", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr4", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr3", Json.array())
                .add("arr2", Json.array(new int[]{1, 2, 3, 4, 5}))
                .add("arr1", Json.array(new int[]{1, 2, 3, 4, 5})));
        LIST.add(Json.object()
                .add("firstObject", Json.object()
                        .add("firstAttribute", Json.object()
                                .add("extraAttr", "extraName"))
                        .add("secondAttribute", 5))
        );
        LIST.add(Json.object()
                .add("a1", true)
                .add("a2", false)
                .add("a3", "v3")
                .add("a4", 4.5)
                .add("a5", Json.NULL));
        LIST.add(Json.object()
                .add("a1", true)
                .add("a2", false)
                .add("arr", Json.array())
                .add("a3", "v3")
                .add("a4", 4.5)
                .add("a5", Json.NULL));
        LIST.add(Json.object()
                .add("a1", true)
                .add("a2", false)
                .add("arr", Json.array()
                        .add(true)
                        .add(false)
                        .add(Json.NULL)
                        .add(12.4).add("asd"))
                .add("a3", "v3")
                .add("a4", 4.5)
                .add("a5", Json.NULL));
        LIST.add(Json.object()
                .add("firstObject", 1)
                .add("second", 2)
                .add("third", Json.object()
                        .add("inner", 5)));
        LIST.add(Json.object()
                .add("this-name-includes-two-byte-utf8-character-£", "this-value-includes-two-byte-utf8-character-£")
                .add("this-name-includes-two-byte-utf8-character-£-2", 2)
                .add("this-name-includes-two-byte-utf8-character-£-3", Json.object()
                        .add("this-name-includes-two-byte-utf8-character-£-inner", 5)));
    }
}
