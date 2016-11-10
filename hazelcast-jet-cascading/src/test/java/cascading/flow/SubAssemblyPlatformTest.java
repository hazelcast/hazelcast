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

package cascading.flow;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.flow.planner.PlannerException;
import cascading.operation.Identity;
import cascading.operation.aggregator.First;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.SumBy;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Ignore;
import org.junit.Test;

/** A planner test only, does not execute */
public class SubAssemblyPlatformTest extends PlatformTestCase
  {
  public SubAssemblyPlatformTest()
    {
    }

  private static class TestAssembly extends SubAssembly
    {
    public TestAssembly( String name )
      {
      this( name, false );
      }

    public TestAssembly( String name, boolean bad )
      {
      Pipe pipe = new Pipe( name );

      pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

      if( !bad )
        setTails( pipe );
      }
    }

  /** Tests that proper pipe graph is assembled without throwing an internal error */
  @Test
  public void testPipeAssembly()
    {
    Pipe pipe = new TestAssembly( "test" );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    Tap source = getPlatform().getTextFile( "foo" );
    Tap sink = getPlatform().getTextFile( "foo/split1", SinkMode.REPLACE );

    List<FlowStep> steps = getPlatform().getFlowConnector().connect( source, sink, pipe ).getFlowSteps();

    assertEquals( "not equal: steps.size()", 1, steps.size() );
    }

  @Test
  public void testBadSubAssembly()
    {
    Pipe pipe = new TestAssembly( "test", true );

    Tap source = getPlatform().getTextFile( "foo" );
    Tap sink = getPlatform().getTextFile( "foo/split1", SinkMode.REPLACE );

    try
      {
      getPlatform().getFlowConnector().connect( source, sink, pipe );
      fail( "did not throw exception" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  @Test
  public void testPipeAssemblySplit()
    {
    Pipe pipe = new TestAssembly( "test" );
    Pipe pipe1 = new GroupBy( "left", pipe, new Fields( "ip" ) );
    Pipe pipe2 = new GroupBy( "right", pipe, new Fields( "ip" ) );

    Tap source = getPlatform().getTextFile( "foo" );
    Tap sink1 = getPlatform().getTextFile( "foo/split1", SinkMode.REPLACE );
    Tap sink2 = getPlatform().getTextFile( "foo/split2", SinkMode.REPLACE );

    Map sources = new HashMap();
    sources.put( "test", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );

    List<FlowStep> steps = getPlatform().getFlowConnector().connect( sources, sinks, pipe1, pipe2 ).getFlowSteps();

    if( getPlatform().isMapReduce() )
      assertEquals( "not equal: steps.size()", 2, steps.size() );
    }

  private static class FirstAssembly extends SubAssembly
    {
    public FirstAssembly( Pipe previous )
      {
      Pipe pipe = new Pipe( "first", previous );

      pipe = new Each( pipe, new Identity() );

      pipe = new GroupBy( pipe, Fields.ALL );

      pipe = new Every( pipe, new First(), Fields.RESULTS );

      setTails( pipe );
      }
    }

  private static class SecondAssembly extends SubAssembly
    {
    public SecondAssembly( Pipe previous )
      {
      Pipe pipe = new Pipe( "second", previous );

      pipe = new Each( pipe, new Identity() );

      pipe = new FirstAssembly( pipe );

      setTails( pipe );
      }
    }

  @Test
  public void testNestedAssembliesAccessors() throws IOException
    {
    Pipe pipe = new Pipe( "test" );

    pipe = new SecondAssembly( pipe );

    Pipe[] allPrevious = pipe.getPrevious();

    assertEquals( "wrong number of previous", 1, allPrevious.length );

//    for( Pipe previous : allPrevious )
//      assertFalse( previous instanceof PipeAssembly );

    Pipe[] heads = pipe.getHeads();

    assertEquals( "wrong number of heads", 1, heads.length );

    for( Pipe head : heads )
      assertFalse( head instanceof SubAssembly );

    }

  @Test
  public void testNestedAssemblies() throws IOException
    {
    Tap source = getPlatform().getTextFile( "foo" );
    Tap sink = getPlatform().getTextFile( "foo/split1", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new SecondAssembly( pipe );

    pipe = new GroupBy( pipe, Fields.size( 1 ) );

    try
      {
      Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

      List<FlowStep> steps = flow.getFlowSteps();

      if( getPlatform().isMapReduce() )
        assertEquals( "wrong size", 2, steps.size() );
      }
    catch( PlannerException exception )
      {
//      exception.writeDOT( "nestedassembly.dot" );

      throw exception;
      }
    }

  @Test
  public void testAssemblyPlanFailure()
    {
    Tap source = getPlatform().getDelimitedFile( new Fields( "date", "size" ), "\t", "someinput" );

    Tap sink = getPlatform().getTextFile( "outpath", SinkMode.REPLACE );
    Tap sink2 = getPlatform().getTextFile( "outpath2", SinkMode.REPLACE );

    Pipe assembly = new Pipe( "assembly" );

    Pipe assembly2 = new Pipe( "assembly2", assembly );

    Fields groupingFields = new Fields( "date" );

    assembly = new AggregateBy(
      assembly,
      groupingFields,
      new SumBy( new Fields( "size" ), new Fields( "size" ), double.class ),
      new SumBy( new Fields( "size" ), new Fields( "size2" ), double.class ),
      new CountBy( new Fields( "sizes" ) ), new CountBy( new Fields( "sizes2" ) )

    );

    assembly2 = new AggregateBy(
      assembly2,
      groupingFields,
      new SumBy( new Fields( "size" ), new Fields( "size" ), double.class ),
      new SumBy( new Fields( "size" ), new Fields( "size2" ), double.class ),
      new CountBy( new Fields( "sizes" ) ), new CountBy( new Fields( "sizes2" ) )

    );

    Map<String, Tap> sinks = new HashMap<String, Tap>();
    sinks.put( "assembly", sink );
    sinks.put( "assembly2", sink2 );

    FlowConnector flowConnector = getPlatform().getFlowConnector();
    // if you reverse assembly and assembly2 it works:
    //Flow flow = flowConnector.connect("test", source, sinks, assembly, assembly2);
    try
      {
      Flow flow = flowConnector.connect( "test", source, sinks, assembly2, assembly );
      fail();
      }
    catch( Exception exception )
      {
      // do nothing - test passes
      }
    }
  }
