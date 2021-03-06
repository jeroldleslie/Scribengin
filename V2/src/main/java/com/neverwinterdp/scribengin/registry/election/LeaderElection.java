package com.neverwinterdp.scribengin.registry.election;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.neverwinterdp.scribengin.registry.ErrorCode;
import com.neverwinterdp.scribengin.registry.Node;
import com.neverwinterdp.scribengin.registry.NodeCreateMode;
import com.neverwinterdp.scribengin.registry.NodeEvent;
import com.neverwinterdp.scribengin.registry.NodeWatcher;
import com.neverwinterdp.scribengin.registry.Registry;
import com.neverwinterdp.scribengin.registry.RegistryException;

public class LeaderElection {
  private Registry  registry ;
  private String    electionPath ;
  private LeaderId  leaderId ;
  private LeaderElectionListener listener ;
  private boolean elected = false ;
  private Node node ;
  
  public LeaderElection(Registry registry, String electionPath) {
    this.registry = registry;
    this.electionPath = electionPath ;
  }
  
  public Registry getRegistry() { return this.registry ; }
  
  public String getElectionPath() { return this.electionPath ; }
  
  public void setListener(LeaderElectionListener listener) {
    this.listener = listener ;
  }
  
  public boolean isElected() { return this.elected ; }
  
  public LeaderId getLeaderId() { return this.leaderId ; }
  
  public Node getNode() { return this.node; }
  
  public void start() throws RegistryException {
    if(leaderId != null) {
      throw new RegistryException(ErrorCode.Unknown, "This leader election is already started") ;
    }
    String lockPath = electionPath + "/leader-" ;
    node = registry.create(lockPath , new byte[0], NodeCreateMode.EPHEMERAL_SEQUENTIAL);
    leaderId = new LeaderId(node.getPath()) ;
    LeaderWatcher watcher = new LeaderWatcher() ;
    watcher.watch();
  }
  
  public void stop() throws RegistryException {
    registry.delete(leaderId.getPath());
    elected = false ;
    leaderId = null ;
    node = null ;
  }
  
  private SortedSet<LeaderId> getSortedLockIds() throws RegistryException {
    List<String> names = registry.getChildren(electionPath) ;
    SortedSet<LeaderId> sortedLockIds = new TreeSet<LeaderId>();
    for (String nodeName : names) {
      if(nodeName.startsWith("leader-")) {
        sortedLockIds.add(new LeaderId(electionPath + "/" + nodeName));
      }
    }
    return sortedLockIds;
  }
  
  class LeaderWatcher implements NodeWatcher {
    @Override
    public void process(NodeEvent event) {
      try {
        watch() ;
      } catch(RegistryException ex) {
        throw new RuntimeException(ex) ;
      }
    }
    
    public void watch() throws RegistryException {
      SortedSet<LeaderId> leaderIds = getSortedLockIds() ;
      LeaderId ownerId = leaderIds.first() ;
      if(ownerId.equals(leaderId)) {
        listener.onElected();
        elected = true ;
        return ;
      }
      SortedSet<LeaderId> lessThanMe = leaderIds.headSet(leaderId);
      LeaderId previousLock = lessThanMe.last();
      registry.watch(previousLock.getPath(), this);
    }
  }
}
