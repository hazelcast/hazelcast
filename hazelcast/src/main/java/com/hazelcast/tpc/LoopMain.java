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

package com.hazelcast.tpc;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.nio.NioEventloop;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class LoopMain {

    public static void main(String[] args) {
        Eventloop eventloop = new NioEventloop();
        eventloop.start();

        eventloop.execute(() -> eventloop.unsafe().loop(new FunctionEx<>() {
            int x = 0;

            @Override
            public Boolean applyEx(Eventloop eventloop1) {
                System.out.println(x);
                if (x < 100) {
                    x++;
                    return true;
                } else {
                    return false;
                }
            }
        }).then((BiConsumer<Object, Throwable>) (o, o2) -> System.out.println("Ready")));
    }
}
