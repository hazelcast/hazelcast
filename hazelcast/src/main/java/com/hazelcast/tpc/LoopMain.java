package com.hazelcast.tpc;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.nio.NioEventloop;

import java.util.ArrayList;
import java.util.List;

public class LoopMain {

    public static void main(String[] args) {
        Eventloop eventloop = new NioEventloop();
        eventloop.start();

        eventloop.execute(() -> eventloop.unsafe.loop(new FunctionEx<>() {
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
        }).then(o -> System.out.println("Loop ready")));

        List<Integer> input = new ArrayList<>();
        input.add(1);
        input.add(2);
        input.add(3);

        eventloop.execute(() -> eventloop.unsafe.map(input, new ArrayList<>(), (FunctionEx<Integer, Integer>) integer -> integer * 2)
                .then(integers -> System.out.println(integers)));

    }

}
