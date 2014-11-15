package com.neverwinterdp.scribengin.dataflow;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.neverwinterdp.scribengin.scribe.Scribe;
import com.neverwinterdp.scribengin.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.source.UUIDSourceStream;
import com.neverwinterdp.scribengin.streamcoordinator.DumbStreamCoordinator;

public class DataflowTest {
  @Test
  public void testScribe() throws Exception {
    
    Dataflow d = new DataflowImpl("Test", new DumbStreamCoordinator(5));
    
    d.initScribes();
    d.start();
    Thread.sleep(1500);
    d.pause();
    //Gives threads a chance to end and catch up
    Thread.sleep(100);

    Scribe[] scribes = d.getScribes();
    for(Scribe s: scribes){
      assertEquals( ((UUIDSourceStream)s.getStream().getSourceStream()).getNumTuples(),
          ((InMemorySinkStream)s.getStream().getInvalidSink()).getData().size() +
          ((InMemorySinkStream)s.getStream().getSinkStream()).getData().size());
    }

    d.start();
    Thread.sleep(1500);
    d.pause();
    //Gives threads a chance to end and catch up
    Thread.sleep(100);

    for(Scribe s: scribes){
      assertEquals( ((UUIDSourceStream)s.getStream().getSourceStream()).getNumTuples(),
          ((InMemorySinkStream)s.getStream().getInvalidSink()).getData().size() +
          ((InMemorySinkStream)s.getStream().getSinkStream()).getData().size());
    }


    d.stop();
    assertEquals("Test", d.getName());
  }
}