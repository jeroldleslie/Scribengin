package com.neverwinterdp.scribengin.source;
/**
 * @author Tuan Nguyen
 */
public interface Source {
  public SourceDescriptor getSourceDescriptor() ;
  public SourceStream   getSourceStream(int id) ;
  public SourceStream   getSourceStream(SourceStreamDescriptor descriptor) ;
  
  public SourceStream[] getSourceStreams() ;
}