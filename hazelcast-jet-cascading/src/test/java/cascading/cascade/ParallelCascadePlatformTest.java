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

import cascading.ComparePlatformsTest;
import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.FieldJoiner;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Ignore;
import org.junit.Test;

import static data.InputData.inputFileIps;

public class ParallelCascadePlatformTest extends PlatformTestCase
  {
  public ParallelCascadePlatformTest()
    {
    super( true );
    }

  private Flow firstFlow( String name )
    {
    Tap source = getPlatform().getTextFile( inputFileIps );

    Pipe pipe = new Pipe( name );

    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "ip" ) ), new Fields( "ip" ) );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "ip" ), getOutputPath( name ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow secondFlow( String name, Tap source )
    {
    Pipe pipe = new Pipe( name );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third", "fourth" ), "\\." ) );
    pipe = new Each( pipe, new FieldJoiner( new Fields( "mangled" ), "-" ) );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "mangled" ), getOutputPath( name ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow thirdFlow( Tap lhs, Tap rhs )
    {
    Pipe lhsPipe = new Pipe( "lhs" );
    Pipe rhsPipe = new Pipe( "rhs" );

    Pipe pipe = new CoGroup( lhsPipe, new Fields( 0 ), rhsPipe, new Fields( 0 ), Fields.size( 2 ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "third" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( Cascades.tapsMap( Pipe.pipes( lhsPipe, rhsPipe ), Tap.taps( lhs, rhs ) ), sink, pipe );
    }

  @Test
  public void testCascade() throws IOException
    {
    getPlatform().copyFromLocal( inputFileIps );

    Flow first1 = firstFlow( "first1" );
    Flow second1 = secondFlow( "second1", first1.getSink() );

    Flow first2 = firstFlow( "first2" );
    Flow second2 = secondFlow( "second2", first2.getSink() );

    Flow third = thirdFlow( second1.getSink(), second2.getSink() );

    Cascade cascade = new CascadeConnector( getProperties() ).connect( first1, second1, first2, second2, third );

    cascade.start();

    cascade.complete();

    validateLength( third, 28 );
    }

  @Test
  public void testCascadeRaceCondition() throws Throwable
    {
    getPlatform().copyFromLocal( inputFileIps );

    final Throwable[] found = new Throwable[ 1 ];

    CascadeListener listener = new CascadeListener()
      {
      @Override
      public void onStarting( Cascade cascade )
        {
        }

      @Override
      public void onStopping( Cascade cascade )
        {
        }

      @Override
      public void onCompleted( Cascade cascade )
        {
        }

      @Override
      public boolean onThrowable( Cascade cascade, Throwable throwable )
        {
        found[ 0 ] = throwable;
        return false;
        }
      };

    for( int i = 0; i <= 500; i += 50 )
      {
      Flow first = firstFlow( String.format( "race-%d/first" + ComparePlatformsTest.NONDETERMINISTIC, i ) );

      Cascade cascade = new CascadeConnector( getProperties() ).connect( first );

      cascade.addListener( listener );

      cascade.start();

      Thread.sleep( i );

      cascade.stop();

      cascade.complete();

      if( found[ 0 ] != null )
        throw found[ 0 ];
      }
    }
  }