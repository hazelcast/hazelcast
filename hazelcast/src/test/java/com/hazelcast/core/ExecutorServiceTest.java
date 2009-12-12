/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

import com.hazelcast.impl.ExecutorManager;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.AfterClass;
import static junit.framework.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * Testing suite for ExecutorService
 * 
 * @author Marco Ferrante, DISI - University of Genoa
 */
public class ExecutorServiceTest {

	public static int COUNT = 1000;
	/**
	 * Get a service instance.
	 */
    @Test
    public void testGetExecutorService() {
        ExecutorService executor = Hazelcast.getExecutorService();
        assertNotNull(executor);
    }

    static class BasicTestTask implements Callable<String>, Serializable {
    	
    	public static String RESULT = "Task completed";
    	
       	public String call() throws Exception {
    		return RESULT;
    	}
    }
    
    /**
     * Submit a null task must raise a NullPointerException
     */
    @Test
    public void submitNullTask() throws Exception {
        ExecutorService executor = Hazelcast.getExecutorService();
        try {
        	Callable c = null;
        	Future future = executor.submit(c);
        	fail();
        }
        catch (NullPointerException npe) {
        	; // It's ok
        }
    }
    
    /**
     * Run a basic task
     */
    @Test
    public void testBasicTask() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = Hazelcast.getExecutorService();
        Future future = executor.submit(task);
        assertEquals(future.get(), BasicTestTask.RESULT);
    }
    
    /**
     * Test the method isDone()
     */
    @Test
    public void isDoneMethod() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = Hazelcast.getExecutorService();
        Future future = executor.submit(task);
        if (future.isDone()) {
        	assertTrue(future.isDone());
        }
        assertEquals(future.get(), BasicTestTask.RESULT);
    	assertTrue(future.isDone());
    }

    /**
     * Test for the issue 129.
     * Repeadetly runs tasks and check for isDone() status after
     * get(). 
     */
    @Test
    public void idDoneMethod() throws Exception {
    	ExecutorService executor = Hazelcast.getExecutorService();
    	for (int i = 0; i < COUNT; i++) {
            Callable<String> task1 = new BasicTestTask();
            Callable<String> task2 = new BasicTestTask();
            
            Future future1 = executor.submit(task1);
            Future future2 = executor.submit(task1);
            assertEquals(future2.get(), BasicTestTask.RESULT);
        	assertTrue(future2.isDone());
            assertEquals(future1.get(), BasicTestTask.RESULT);
        	assertTrue(future1.isDone());
    	}
    }

    /**
     * Test multiple Future.get() invokation 
     */
    @Test
    public void isTwoGetFromFuture() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = Hazelcast.getExecutorService();
        Future<String> future = executor.submit(task);
        String s1 = future.get();
        assertEquals(s1, BasicTestTask.RESULT);
    	assertTrue(future.isDone());
        String s2 = future.get();
        assertEquals(s2, BasicTestTask.RESULT);
    	assertTrue(future.isDone());
        String s3 = future.get();
        assertEquals(s3, BasicTestTask.RESULT);
    	assertTrue(future.isDone());
        String s4 = future.get();
        assertEquals(s4, BasicTestTask.RESULT);
    	assertTrue(future.isDone());
    }

    
    /**
     * invokeAll tests
     */
    @Test
    public void testInvokeAll() throws Exception {
        Callable<String> task = new BasicTestTask();
        ExecutorService executor = Hazelcast.getExecutorService();
        assertFalse(executor.isShutdown());
        
        // Only one task
        ArrayList<Callable<String>> tasks = new ArrayList<Callable<String>>();
        tasks.add(new BasicTestTask());
        List<Future<String>> futures = executor.invokeAll(tasks);
        assertEquals(futures.size(), 1);
        assertEquals(futures.get(0).get(), BasicTestTask.RESULT);
        
        // More tasks
        tasks.clear();
        for (int i = 0; i < COUNT; i++) {
            tasks.add(new BasicTestTask());
        }
        futures = executor.invokeAll(tasks);
        assertEquals(futures.size(), COUNT);
        for (int i = 0; i < COUNT; i++) {
        	assertEquals(futures.get(i).get(), BasicTestTask.RESULT);
        }
    }
    
    /**
     * Shutdown-related method behaviour when the cluster is running
     */
    @Test
    public void testShutdownBehaviour() throws Exception {
        ExecutorService executor = Hazelcast.getExecutorService();
        
        // Fresh instance, is not shutting down
        assertFalse(executor.isShutdown());
        assertFalse(executor.isTerminated());
        
        // shutdown() should be ignored
        executor.shutdown();
        assertFalse(executor.isShutdown());
        assertFalse(executor.isTerminated());

        // shutdownNow() should return an empty list and be ignored 
        List<Runnable> pending = executor.shutdownNow();
        assertTrue(pending.isEmpty());
        assertFalse(executor.isShutdown());
        assertFalse(executor.isTerminated());

        // awaitTermination() should return immediately false
        try {
	        boolean terminated = executor.awaitTermination(60L, TimeUnit.SECONDS);
	        assertFalse(terminated);
        }
        catch (InterruptedException ie) {
        	fail("InterruptedException");
        }
        assertFalse(executor.isShutdown());
        assertFalse(executor.isTerminated());
    }
    
    /**
     * Shutting down the cluster should act as the ExecutorService shutdown
     */
    @Test
    public void testClusterShutdown() throws Exception {
        ExecutorService executor = Hazelcast.getExecutorService();
        
        Hazelcast.shutdown();
        assertNotNull(executor);
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());
        
        // New tasks must be rejected
        Callable<String> task = new BasicTestTask();
        try {
        	Future future = executor.submit(task);
        	fail("Should not be here!");
        }
        catch (RejectedExecutionException ree) {
        	; // It's ok
        }
        
        // Reanimate Hazelcast
        Hazelcast.restart();
        executor = Hazelcast.getExecutorService();
        assertFalse(executor.isShutdown());
        assertFalse(executor.isTerminated());
        
      	Future future = executor.submit(task);
    }
    
}
