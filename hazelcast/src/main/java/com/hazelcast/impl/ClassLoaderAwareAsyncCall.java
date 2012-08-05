package com.hazelcast.impl;

public abstract class ClassLoaderAwareAsyncCall extends AsyncCall {

  private ClassLoader classLoader;
  
  public ClassLoaderAwareAsyncCall(){
      classLoader = Thread.currentThread().getContextClassLoader();
  }
  
  public ClassLoaderAwareAsyncCall(ClassLoader cl){
      classLoader = cl;
  }
  
  @Override
  public void run(){
      ClassLoader current = Thread.currentThread().getContextClassLoader();
      try{
          Thread.currentThread().setContextClassLoader(classLoader);
          super.run();
      } finally{
          if(current != null){
              Thread.currentThread().setContextClassLoader(current);
          }
      }
  }
  
}
