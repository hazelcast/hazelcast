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

package cascading.pipe.checkpoint;

import java.util.List;

import cascading.PlatformTestCase;
import cascading.TestFunction;
import cascading.flow.Flow;
import cascading.flow.FlowConnectorProps;
import cascading.flow.FlowDef;
import cascading.flow.FlowStep;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Checkpoint;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.DecoratorTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static cascading.flow.FlowDef.flowDef;
import static data.InputData.inputFileApache;

/**
 *
 */
public class CheckpointPlatformTest extends PlatformTestCase
  {
  public CheckpointPlatformTest()
    {
    super( true ); // leave cluster testing enabled
    }

  @Test
  public void testSimpleCheckpoint() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Checkpoint( pipe );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "simplecheckpoint" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8, null );

    if( !getPlatform().isMapReduce() )
      return;

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "wrong size", 2, steps.size() );
    }

  @Test
  public void testManyCheckpoints() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    { // job 1
    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Checkpoint( pipe );
    }

    { // job 2
    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    pipe = new Checkpoint( pipe );

    new FlowConnectorProps()
      .setCheckpointTapDecoratorClassName( DecoratorTap.class )
      .setProperties( pipe.getConfigDef() );
    }

    { // job 3
    pipe = new Each( pipe, new Identity() );

    pipe = new Checkpoint( pipe ); // this should be collapsed into the sink tap, not be a fourth job
    }

    Tap sink = getPlatform().getTextFile( getOutputPath( "manycheckpoint" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8, null );

    if( !( getPlatform().isMapReduce() ) )
      return;

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "wrong size", 3, steps.size() );

    int count = 0;
    for( FlowStep step : steps )
      {
      if( step.getSink() instanceof DecoratorTap )
        count++;
      }

    assertEquals( 1, count );
    }

  @Test
  public void testSimpleCheckpointTextIntermediate() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Checkpoint( "checkpoint", pipe );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "checkpoint/sink" ), SinkMode.REPLACE );

    Tap checkpoint = getPlatform().getDelimitedFile( Fields.ALL, true, "\t", "\"", getOutputPath( "checkpoint/tap" ), SinkMode.REPLACE );

    FlowDef flowDef = flowDef()
      .addSource( pipe, source )
      .addTailSink( pipe, sink )
      .addCheckpoint( "checkpoint", checkpoint );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 8 );

    if( !( getPlatform().isMapReduce() ) )
      return;

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "wrong size", 2, steps.size() );

    validateLength( flow.openTapForRead( checkpoint ), 10 );
    }

  @Test
  public void testFailCheckpoint() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Checkpoint( "checkpoint", pipe );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "failcheckpoint/sink" ), SinkMode.REPLACE );

    Tap checkpoint = getPlatform().getDelimitedFile( Fields.ALL, true, "\t", "\"", getOutputPath( "failcheckpoint/tap" ), SinkMode.REPLACE );

    FlowDef flowDef = flowDef()
      .addSource( pipe, source )
      .addTailSink( pipe, sink )
      .addCheckpoint( "checkpointXXXXX", checkpoint );

    try
      {
      Flow flow = getPlatform().getFlowConnector().connect( flowDef );
      fail();
      }
    catch( Exception exception )
      {
//      exception.printStackTrace();
      // do nothing
      }
    }

  @Test
  public void testFailCheckpointBeforeEvery() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Checkpoint( "checkpoint", pipe );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "failcheckpointevery/sink" ), SinkMode.REPLACE );

    Tap checkpoint = getPlatform().getDelimitedFile( Fields.ALL, true, "\t", "\"", getOutputPath( "failcheckpointevery/tap" ), SinkMode.REPLACE );

    FlowDef flowDef = flowDef()
      .addSource( pipe, source )
      .addTailSink( pipe, sink )
      .addCheckpoint( "checkpoint", checkpoint );

    try
      {
      Flow flow = getPlatform().getFlowConnector().connect( flowDef );
      fail();
      }
    catch( Exception exception )
      {
//      exception.printStackTrace();
      // do nothing
      }
    }

  @Test
  public void testFailCheckpointDeclaredFields() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Checkpoint( "checkpoint", pipe );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "failcheckpointdeclared/sink" ), SinkMode.REPLACE );

    Tap checkpoint = getPlatform().getTextFile( getOutputPath( "failcheckpointdeclared/tap" ), SinkMode.REPLACE );

    FlowDef flowDef = flowDef()
      .addSource( pipe, source )
      .addTailSink( pipe, sink )
      .addCheckpoint( "checkpoint", checkpoint );

    try
      {
      Flow flow = getPlatform().getFlowConnector().connect( flowDef );
      fail();
      }
    catch( Exception exception )
      {
//      exception.printStackTrace();
      // do nothing
      }
    }

  @Test
  public void testDuplicateCheckpoint() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Checkpoint( "checkpoint", pipe );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    pipe = new Checkpoint( "checkpoint", pipe );

    Tap sink = getPlatform().getTextFile( getOutputPath( "duplicatecheckpoint" ), SinkMode.REPLACE );

    FlowDef flowDef = FlowDef.flowDef()
      .setName( "restartable" )
      .addSource( "test", source )
      .addTailSink( pipe, sink )
      .setRunID( "restartable" );

    try
      {
      Flow flow = getPlatform().getFlowConnector().connect( flowDef );
      fail( "should throw element graph exception" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  @Test
  public void testRestartCheckpoint() throws Exception
    {
    if( !getPlatform().isMapReduce() )
      return;

    getPlatform().copyFromLocal( inputFileApache );

    String sinkPath = getOutputPath( "restartcheckpoint" );

    Flow flow = createRestartableFlow( sinkPath, true );

    try
      {
      flow.complete();
      fail( "flow should fail" );
      }
    catch( Exception exception )
      {
      // ignored
      }

    int count = 0;
    List<FlowStep> steps = flow.getFlowSteps();

    for( FlowStep step : steps )
      {
      Tap sink = step.getSink();
      if( flow.getSink() != sink && sink.resourceExists( flow.getConfig() ) )
        count++;
      }

    assertEquals( "wrong number of intermediate resources exist", 1, count );

    flow = createRestartableFlow( sinkPath, false );

    flow.complete();

    validateLength( flow, 8, null );

    assertEquals( "wrong size", 2, flow.getFlowSteps().size() );
    }

  private Flow createRestartableFlow( String sinkPath, boolean fail )
    {
    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Checkpoint( pipe );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    pipe = new Each( pipe, new TestFunction( new Fields( "insert" ), new Tuple( "value" ), fail ? 2 : -1 ) );

    Tap sink = getPlatform().getTextFile( sinkPath, SinkMode.REPLACE );

    FlowDef flowDef = FlowDef.flowDef()
      .setName( "restartable" )
      .addSource( "test", source )
      .addTailSink( pipe, sink )
      .setRunID( "restartable" );

    return getPlatform().getFlowConnector().connect( flowDef );
    }
  }