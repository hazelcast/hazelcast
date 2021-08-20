package com.hazelcast.jet.sql.impl;

import java.util.HashMap;

public class LogTimer {
    static final HashMap<String, Long> Timers= new HashMap<String, Long>();
    public static boolean active = false;

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
        long resultNano = nanoTime -Timers.get(timerName);
        System.out.println("ttt == "+timerName+" took "+resultNano+" nanosecs\n\t\t"
                +(resultNano/1000)+" microSecs\n\t\t"
                +(resultNano/1000000)+" milliSecs\n\t\t"
                +"Time == " + nanoTime);
    }
}
