/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.cascade;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import cascading.CascadingException;
import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.flow.LockingFlowListener;
import cascading.flow.process.ProcessFlow;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.FieldJoiner;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.stats.FlowStats;
import cascading.stats.process.ProcessStepStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;
import riffle.process.DependencyIncoming;
import riffle.process.DependencyOutgoing;
import riffle.process.ProcessChildren;
import riffle.process.ProcessComplete;
import riffle.process.ProcessConfiguration;
import riffle.process.ProcessCounters;
import riffle.process.ProcessStart;
import riffle.process.ProcessStop;
import riffle.process.scheduler.ProcessChain;

import static data.InputData.inputFileIps;

public class RiffleCascadePlatformTest extends PlatformTestCase
  {
  public RiffleCascadePlatformTest()
    {
    super( false );
    }

  private Flow firstFlow( String path )
    {
    Tap source = getPlatform().getTextFile( inputFileIps );

    Pipe pipe = new Pipe( "first" );

    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "ip" ) ), new Fields( "ip" ) );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "ip" ), getOutputPath( path + "/first" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow secondFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "second" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third", "fourth" ), "\\." ) );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "first", "second", "third", "fourth" ), getOutputPath( path + "/second" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow thirdFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "third" );

    pipe = new Each( pipe, new FieldJoiner( new Fields( "mangled" ), "-" ) );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "mangled" ), getOutputPath( path + "/third" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow fourthFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "fourth" );

    pipe = new Each( pipe, new Identity() );

    Tap sink = getPlatform().getTextFile( getOutputPath( path + "/fourth" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  @Test
  public void testSimpleRiffle() throws IOException
    {
    getPlatform().copyFromLocal( inputFileIps );

    String path = "perpetual";

    Flow first = firstFlow( path );
    Flow second = secondFlow( first.getSink(), path );
    Flow third = thirdFlow( second.getSink(), path );
    Flow fourth = fourthFlow( third.getSink(), path );

    ProcessChain chain = new ProcessChain( true, fourth, second, first, third );

    chain.start();

    chain.complete();

    validateLength( fourth, 20 );
    }

  @Test
  public void testSimpleRiffleCascade() throws IOException, InterruptedException
    {
    getPlatform().copyFromLocal( inputFileIps );

    String path = "perpetualcascade";

    Flow first = firstFlow( path );
    Flow second = secondFlow( first.getSink(), path );
    Flow third = thirdFlow( second.getSink(), path );
    Flow fourth = fourthFlow( third.getSink(), path );

    ProcessFlow firstProcess = new ProcessFlow( "first", first );
    ProcessFlow secondProcess = new ProcessFlow( "second", second );
    ProcessFlow thirdProcess = new ProcessFlow( "third", third );
    ProcessFlow fourthProcess = new ProcessFlow( "fourth", fourth );

    LockingFlowListener flowListener = new LockingFlowListener();
    secondProcess.addListener( flowListener );

    Cascade cascade = new CascadeConnector( getProperties() ).connect( fourthProcess, secondProcess, firstProcess, thirdProcess );

    cascade.start();

    cascade.complete();

    assertTrue( "did not start", flowListener.started.tryAcquire( 2, TimeUnit.SECONDS ) );
    assertTrue( "did not complete", flowListener.completed.tryAcquire( 2, TimeUnit.SECONDS ) );

    validateLength( fourth, 20 );
    }

  @Test
  public void testProcessFlowFlowListenerExceptionHandlingInStart() throws IOException, InterruptedException
    {

    ThrowableListener listener = new ThrowableListener();

    getPlatform().copyFromLocal( inputFileIps );

    String path = "startException";

    Flow process = flowWithException( path, FailingRiffle.Failing.START );
    process.addListener( listener );

    try
      {
      process.start();
      fail( "there should have been an exception" );
      }
    catch( CascadingException exception )
      {
      assertNotNull( listener.getThrowable() );
      }

    }

  @Test
  public void testProcessFlowFlowListenerExceptionHandlingInComplete() throws IOException, InterruptedException
    {

    ThrowableListener listener = new ThrowableListener();

    getPlatform().copyFromLocal( inputFileIps );

    String path = "completeException";

    Flow process = flowWithException( path, FailingRiffle.Failing.COMPLETE );
    process.addListener( listener );

    try
      {
      process.complete();
      fail( "there should have been an exception" );
      }
    catch( CascadingException exception )
      {
      assertNotNull( listener.getThrowable() );
      }
    }

  @Test
  public void testProcessFlowWithCounters() throws IOException
    {
    getPlatform().copyFromLocal( inputFileIps );
    Map<String, Map<String, Long>> counters = new HashMap<String, Map<String, Long>>();

    Map<String, Long> innerMap = new HashMap<String, Long>();
    innerMap.put( "inner-key", 42L );

    counters.put( "outer-key", innerMap );

    Flow process = flowWithCounters( "counter", counters );
    process.complete();
    FlowStats flowStats = process.getFlowStats();
    assertNotNull( flowStats );

    List children = new ArrayList( flowStats.getChildren() );
    assertEquals( 1, children.size() );
    ProcessStepStats stepStats = (ProcessStepStats) children.get( 0 );
    assertEquals( counters.keySet(), stepStats.getCounterGroups() );
    assertEquals( innerMap.keySet(), stepStats.getCountersFor( "outer-key" ) );
    assertEquals( 42L, stepStats.getCounterValue( "outer-key", "inner-key" ) );
    }

  @Test
  public void testProcessFlowWithChildCounters() throws IOException
    {
    getPlatform().copyFromLocal( inputFileIps );
    Map<String, Map<String, Long>> counters = new HashMap<String, Map<String, Long>>();

    Map<String, Long> innerMap = new HashMap<String, Long>();
    innerMap.put( "inner-key", 42L );

    counters.put( "outer-key", innerMap );

    Flow process = flowWithChildren( "children", counters );
    process.complete();
    FlowStats flowStats = process.getFlowStats();
    assertNotNull( flowStats );

    List children = new ArrayList( flowStats.getChildren() );
    assertEquals( 1, children.size() );
    ProcessStepStats stepStats = (ProcessStepStats) children.get( 0 );
    assertEquals( counters.keySet(), stepStats.getCounterGroups() );
    assertEquals( innerMap.keySet(), stepStats.getCountersFor( "outer-key" ) );
    assertEquals( 42L, stepStats.getCounterValue( "outer-key", "inner-key" ) );
    }

  @Test
  public void testProcessFlowFlowListenerExceptionHandlingInStop() throws IOException, InterruptedException
    {
    ThrowableListener listener = new ThrowableListener();

    getPlatform().copyFromLocal( inputFileIps );

    String path = "stopException";

    Flow process = flowWithException( path, FailingRiffle.Failing.STOP );
    process.addListener( listener );
    process.start();

    try
      {
      process.stop();
      fail( "there should have been an exception" );
      }
    catch( CascadingException exception )
      {
      assertNotNull( listener.getThrowable() );
      }
    }

  private Flow flowWithException( String path, FailingRiffle.Failing failing )
    {
    Tap source = getPlatform().getTextFile( inputFileIps );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "ip" ), getOutputPath( path + "/first" ), SinkMode.REPLACE );

    return new ProcessFlow( "flow", new FailingRiffle( source, sink, failing ) );
    }

  private Flow flowWithCounters( String path, Map<String, Map<String, Long>> counters )
    {
    Tap source = getPlatform().getTextFile( inputFileIps );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "ip" ), getOutputPath( path + "/first" ), SinkMode.REPLACE );

    return new ProcessFlow( "counter-flow", new CounterRiffle( source, sink, counters ) );
    }

  private Flow flowWithChildren( String path, Map<String, Map<String, Long>> counters )
    {
    Tap source = getPlatform().getTextFile( inputFileIps );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "ip" ), getOutputPath( path + "/first" ), SinkMode.REPLACE );

    return new ProcessFlow( "counter-flow", new ChildCounterRiffle( source, sink, counters ) );
    }

  @riffle.process.Process
  static class FailingRiffle
    {

    enum Failing
      {
        START, STOP, COMPLETE
      }

    Tap sink;
    Tap source;
    Failing failing;

    FailingRiffle( Tap source, Tap sink, Failing failing )
      {
      this.source = source;
      this.sink = sink;
      this.failing = failing;
      }

    @ProcessStart
    public void start()
      {
      if( failing == Failing.START )
        crash();
      }

    @ProcessStop
    public void stop()
      {
      if( failing == Failing.STOP )
        crash();
      }

    private void crash()
      {
      throw new CascadingException( "testing" );
      }

    @ProcessComplete
    public void complete()
      {
      if( failing == Failing.COMPLETE )
        crash();
      }

    @ProcessConfiguration
    public Object getConfiguration()
      {
      return new Properties();
      }

    @DependencyOutgoing
    public Collection getOutgoing()
      {
      return Collections.unmodifiableCollection( Arrays.asList( sink ) );
      }

    @DependencyIncoming
    public Collection getIncoming()
      {
      return Collections.unmodifiableCollection( Arrays.asList( source ) );
      }
    }

  class ThrowableListener implements FlowListener
    {
    public Throwable throwable;

    @Override
    public void onStarting( Flow flow )
      {

      }

    @Override
    public void onStopping( Flow flow )
      {

      }

    @Override
    public void onCompleted( Flow flow )
      {

      }

    @Override
    public boolean onThrowable( Flow flow, Throwable throwable )
      {
      this.throwable = throwable;
      return true;
      }

    public Throwable getThrowable()
      {
      return throwable;
      }
    }

  @riffle.process.Process
  class CounterRiffle
    {
    Tap sink;
    Tap source;
    Map<String, Map<String, Long>> counter;

    CounterRiffle( Tap source, Tap sink, Map<String, Map<String, Long>> counter )
      {
      this.source = source;
      this.sink = sink;
      this.counter = counter;
      }

    @ProcessStart
    public void start()
      {
      }

    @ProcessStop
    public void stop()
      {
      }

    @ProcessCounters
    public Map<String, Map<String, Long>> getCounters()
      {
      return counter;
      }

    @ProcessComplete
    public void complete()
      {
      }

    @DependencyOutgoing
    public Collection getOutgoing()
      {
      return Collections.unmodifiableCollection( Arrays.asList( sink ) );
      }

    @DependencyIncoming
    public Collection getIncoming()
      {
      return Collections.unmodifiableCollection( Arrays.asList( source ) );
      }

    @ProcessConfiguration
    public Object getConfiguration()
      {
      return new Properties();
      }
    }

  @riffle.process.Process
  class ChildCounterRiffle
    {
    Tap sink;
    Tap source;
    Map<String, Map<String, Long>> counter;

    ChildCounterRiffle( Tap source, Tap sink, Map<String, Map<String, Long>> counter )
      {
      this.source = source;
      this.sink = sink;
      this.counter = counter;
      }

    @ProcessStart
    public void start()
      {
      }

    @ProcessStop
    public void stop()
      {
      }

    @ProcessCounters
    public Map<String, Map<String, Long>> getCounters()
      {
      return null;
      }

    @ProcessChildren
    public List<Object> getChildren()
      {
      return Arrays.<Object>asList( new CounterRiffle( source, sink, counter ) );
      }

    @ProcessComplete
    public void complete()
      {
      }

    @DependencyOutgoing
    public Collection getOutgoing()
      {
      return Collections.unmodifiableCollection( Arrays.asList( sink ) );
      }

    @DependencyIncoming
    public Collection getIncoming()
      {
      return Collections.unmodifiableCollection( Arrays.asList( source ) );
      }

    @ProcessConfiguration
    public Object getConfiguration()
      {
      return new Properties();
      }
    }
  }