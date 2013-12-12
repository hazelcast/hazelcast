package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;

import java.io.Serializable;
import java.util.Random;

public class DemoApp {

    public static void main(String[] args) throws InterruptedException {
        String securityToken = null;
        if (args.length >= 1) {
            securityToken = args[0];
            if (securityToken.equals("null")) {
                securityToken = null;
            }
        }

        String projectId = null;
        if (args.length >= 2) {
            projectId = args[1];
            if (projectId.equals("null")) {
                projectId = null;
            }
        }

        String group = null;
        if (args.length >= 3) {
            group = args[2];
            if (group.equals("null")) {
                group = null;
            }
        }



        System.out.println("Starting demo application with the following settings");
        System.out.println("SecurityToken: "+securityToken);
        System.out.println("ProjectId: "+projectId);
        System.out.println("Group: "+group);

        Config config = new Config();
        config.getManagementCenterConfig().setEnabled(true);
        config.getManagementCenterConfig().setSecurityToken(securityToken);
        config.getManagementCenterConfig().setProjectId(projectId);
        //we need to set a group to prevent forming a cluster with app1
        if(group!=null)
        config.getGroupConfig().setName(group);
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        new MapThread("Map1Tread", hz1.getMap("map1")).start();
        new MapThread("Map2Tread", hz1.getMap("map2")).start();
        new QueueThread("Queue1Thread", hz1.getQueue("queue1")).start();
        new QueueThread("Queue2Thread", hz1.getQueue("queue2")).start();
        new ExecutorThread("Executor1Thread", hz1.getExecutorService("executor1")).start();
        new ExecutorThread("Executor2Thread", hz1.getExecutorService("executor2")).start();
        new TopicThread("Topic1Thread", hz1.getTopic("topic1")).start();
        new TopicThread("Topic2Thread", hz1.getTopic("topic2")).start();
        new MultiMapThread("MultiMap1Thread", hz1.getMultiMap("multimap1")).start();
        new MultiMapThread("MultiMap2Thread", hz1.getMultiMap("multimap2")).start();

        Thread.sleep(1000000000);
    }


    private static class MapThread extends Thread{
        private final IMap map;

        private MapThread(String name, IMap map) {
            super(name);
            this.map = map;
        }

        public void run(){
            int count = 100000;
            for(int k=0;k< count;k++){
                map.put(k,k);
            }

            Random random = new Random();
            for(;;){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

                int key = random.nextInt(count);
                map.put(key,key);
                map.get(key);
            }
        }
    }


    private static class MultiMapThread extends Thread{
        private final MultiMap map;

        private MultiMapThread(String name, MultiMap map) {
            super(name);
            this.map = map;
        }

        public void run(){
            int count = 100000;
            for(int k=0;k< count;k++){
                map.put(k,k);
            }

            Random random = new Random();
            for(;;){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

                int key = random.nextInt(count);
                map.put(key,random.nextInt(10));
                map.get(key);
            }
        }
    }

    private static class QueueThread extends Thread{
        private final IQueue queue;

        private QueueThread(String name, IQueue map) {
            super(name);
            this.queue = map;
        }

        public void run(){
            int count = 100000;
            for(int k=0;k< count;k++){
                queue.offer(k);
            }

            Random random = new Random();
            for(;;){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

                queue.offer(random.nextInt());
                try {
                    queue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
    }

    private static class ExecutorThread extends Thread{
        private final IExecutorService executorService;

        private ExecutorThread(String name, IExecutorService executorService) {
            super(name);
            this.executorService = executorService;
        }

        public void run(){
            Random random = new Random();
            for(;;){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                executorService.submit(new Task());
            }
        }
    }

    private static class TopicThread extends Thread{
        private final ITopic topic;

        private TopicThread(String name, ITopic topic) {
            super(name);
            this.topic = topic;
        }

        public void run(){
            Random random = new Random();
            topic.addMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                }
            });

            for(;;){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

                topic.publish(random.nextInt());
            }
        }
    }


    private static class Task implements Serializable,Runnable{
        @Override
        public void run() {
        }
    }

}