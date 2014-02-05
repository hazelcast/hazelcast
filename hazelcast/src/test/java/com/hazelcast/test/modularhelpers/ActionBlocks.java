package com.hazelcast.test.modularhelpers;

import com.hazelcast.core.*;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

//The idea is that the individual instruction public member variables which represent "function" or "function Objects"
//can be reused, recombined, inside different scenarios,  giving a modular way to programming test, by plugging different
//predefined "blocks" / variables representing small bits of code together.
public class ActionBlocks {

    private String keyPrefix;

    //the ObjName used the get the distributes data structs
    private String ObjName;
    //private String keyPrefix;

    private HazelcastInstance hzInstance;


    //this is the ObjName of the maps, queues, lists ect... that we will be performing operations on.
    //making 2 instances of this class with different names, and passing the 2 instruction instance to a scnario
    //will result in operation on different distributed structures.
    public ActionBlocks(String distributedCollectionName, String keyPrefix){
        this.ObjName=distributedCollectionName;
        this.keyPrefix=keyPrefix;
        allputActions.add(mapPut);
        allputActions.add(queueOffer);
        allputActions.add(topicPub);
        allputActions.add(listAdd);
        allputActions.add(setAdd);
        allputActions.add(repMapPut);
        allputActions.add(lockGet);
    }

    public void setKeyPrefix(String keyPrefix){
        this.keyPrefix = keyPrefix;
    }

    //you can set the HazelcastInstance we are using to perform the operations / Insturctions, we could be putting from a node or a client
    public void setHzInstance(HazelcastInstance hz){
        hzInstance=hz;
    }


    public ArrayList<Action> allputActions = new ArrayList<Action>();

    public Action[] getAllPutActions(){
        return allputActions.toArray(new Action[]{});
    }


    public final ExecutorAction executeOnKeyOwner = new ExecutorAction("executeOnKeyOwner") {

        public void before(){ex = hzInstance.getExecutorService(ObjName);}

        public void call(int i){

            try{

                Future<Integer> f = ex.submitToKeyOwner(new CallCountTask(), keyPrefix +"-"+i);

                try {
                    totalCalls += f.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }catch(RejectedExecutionException e){
                e.printStackTrace();
            }
        }
    };


    //this Instruction will create a map with the given ObjName, and on every step of a Scenario will do a put, using
    //the ActionBlocks instance count number and count as the key.
    //if you used the same ActionBlocks instance .mapPut 2 times in the same Scenario, they will be both putting to the same map (with the same ObjName) using the same key
    //if you used the 2 different instances of mapPut with the same ObjName, they operate on the same map but are using different key
    //values for the put due to the instance count.  so there will be 2 times the map puts, in the test
    public final MapAction mapPut = new MapAction("mapPut") {

        public void before(){map = hzInstance.getMap(ObjName);}//WHAT about moving this into the call, so we get the map every time,  and using a cluster hear so we can get a random working node ?

        public void call(int i){
            map.put(keyPrefix +"-"+i, i);
        }
    };

    public final MapAction mapGet = new MapAction("mapGet") {

        public void before(){map = hzInstance.getMap(ObjName);}

        public void call(int i){
            Object o = map.get(keyPrefix +"-"+i);
            assertEquals(i, o);
        }
    };

    public final MapAction mapRemove = new MapAction("mapRemove") {

        public void before(){map = hzInstance.getMap(ObjName);}

        public void call(int i){
            Object o = map.remove(keyPrefix +"-"+i);
            assertEquals(i, o);
        }
    };


    public final MapAction mapPutAsync = new MapAction("mapPutAsync") {

        public void before(){map = hzInstance.getMap(ObjName);}

        public void call(int i){
            map.putAsync(keyPrefix +"-"+i, i);
        }
    };
       
    public final MapAction mapGetAsync = new MapAction("mapGetAsync") {

        public void before(){map = hzInstance.getMap(ObjName);}

        public void call(int i){
            try{
                Future f = map.getAsync(keyPrefix +"-"+i);
                assertEquals(i, f.get());
            }catch(Exception e){
                System.out.println(ObjName +" iteration="+i);
                e.printStackTrace();
            }
        }
    };

    public final MapAction mapRemoveAsync = new MapAction("mapRemoveAsync") {

        public void before(){map = hzInstance.getMap(ObjName);}

        public void call(int i){
            try{
                Future f = map.removeAsync(keyPrefix +"-"+i);
                assertEquals(i, f.get());
            }catch(Exception e){
                System.out.println(ObjName +" iteration="+i);
                e.printStackTrace();
            }
        }
    };

    public final MapAction mapContainsKey = new MapAction("mapContainsKey") {

        public void before(){map = hzInstance.getMap(ObjName);}

        public void call(int i){
            boolean contains = map.containsKey(keyPrefix + "-" + i);
            assertTrue(this.actionName+" "+map+" "+map.getName()+" not contains key ===>>> "+ keyPrefix+"-"+i,  contains );
        }
    };

    public final QueueAction queueOffer = new QueueAction("queueOffer") {
        public void before(){queue = hzInstance.getQueue(ObjName);}
        public void call(int i){
            queue.offer(i);
        }
    };

    public final TopicAction topicPub = new TopicAction("topicPub") {

        public void before(){
            topic = hzInstance.getTopic(ObjName);
            super.before();
        }

        public void call(int i){
            topic.publish(i);
        }
    };


    public final ListAction listAdd = new ListAction("listAdd") {
        public void before(){list = hzInstance.getList(ObjName);}
        public void call(int i){
            list.add(i);
        }
    };

    public final SetAction setAdd = new SetAction("setAdd") {

        public void before() { set = hzInstance.getSet(ObjName); }

        public void call(int i) {
            set.add(i);
        }
    };


    public final RepMapAction repMapPut = new RepMapAction("repMapPut") {
        public void before(){map = hzInstance.getReplicatedMap(ObjName);}
        public void call(int i){
            map.put(i, i);
        }
    };

    public final LockAction lockGet =  new LockAction("lockGet") {
        public void before(){lock = hzInstance.getLock(ObjName);}
        public void call(int i){
            lock.lock();
        }
    };

}
