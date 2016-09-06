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

package cascading.tap;

import java.io.IOException;

import cascading.PlatformTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.junit.Test;

/**
 *
 */
public class TapCollectorPlatformTest extends PlatformTestCase
  {
  public TapCollectorPlatformTest()
    {
    }

  @Test
  public void testTapCollectorText() throws IOException
    {
    Tap tap = getPlatform().getTextFile( getOutputPath( "tapcollectortext" ) );

    runTest( tap );
    }

  @Test
  public void testTapCollectorSequence() throws IOException
    {
    Tap tap = getPlatform().getDelimitedFile( new Fields( "string", "value", "number" ), "\t", getOutputPath( "tapcollectorseq" ) );

    runTest( tap );
    }

  private void runTest( Tap tap ) throws IOException
    {
    TupleEntryCollector collector = tap.openForWrite( getPlatform().getFlowProcess() ); // casting for test

    for( int i = 0; i < 100; i++ )
      collector.add( new Tuple( "string", "" + i, i ) );

    collector.close();

    TupleEntryIterator iterator = tap.openForRead( getPlatform().getFlowProcess() );

    int count = 0;
    while( iterator.hasNext() )
      {
      iterator.next();
      count++;
      }

    iterator.close();

    assertEquals( "wrong size", 100, count );
    }
  }
