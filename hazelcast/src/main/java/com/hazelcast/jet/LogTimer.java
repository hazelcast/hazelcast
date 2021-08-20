package com.hazelcast.jet;

import java.util.HashMap;

public class LogTimer {
    static final HashMap<String, Long> Timers= new HashMap<String, Long>();
    public static boolean active = false;

    public static void calculateSelfTime(String name){
        active = true;
        start(name);
        stop(name);
        active = false;

    }

    public static void start(String timerName){
        if (!active)
            return;

        long nanoTime = System.nanoTime();
        System.out.println("ttt == "+timerName+ " Started! time == "+ nanoTime);
        Timers.put(timerName, nanoTime);
    }

    public static void stop(String timerName){
        if (!active)
            return;
        long nanoTime = System.nanoTime();
        long prevTime = Timers.get(timerName);
        long resultNano = nanoTime - prevTime;
        System.out.println("ttt == "+timerName+" took "+resultNano+" nanosecs\n\t\t"
                +(resultNano/1000)+" microSecs\n\t\t"
                +(resultNano/1000000)+" milliSecs\n\t\t"
                +"Time == " + nanoTime);
    }
}
