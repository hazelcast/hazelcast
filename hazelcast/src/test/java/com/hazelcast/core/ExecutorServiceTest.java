/*
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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
	
	/**
	 * Get a service instance.
	 */
    @Test
    public void testGetExecutorService() {
        ExecutorService executor = Hazelcast.getExecutorService();
        assertNotNull(executor);
    }

    static class BasicTestTask implements Callable<String>, Serializable {
    	
    	public static String RESULT = "Done";
    	
       	public String call() throws Exception {
    		return RESULT;
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
     * Shutdown behaviour
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
        
        // Shutting down the cluster should act as the ExecutorService shutdown
        Hazelcast.shutdown();
        assertNotNull(executor);
        assertNotNull(ExecutorManager.get());
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());
        
    }
    
    
}