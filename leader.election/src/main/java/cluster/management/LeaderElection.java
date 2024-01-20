package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {

    private static final String ELECTION_NAMESPACE = "/election";
    private ZooKeeper zooKeeper;
    private String currentZnodeName;

    private OnElectionCallback onElectionCallback;

    public LeaderElection(ZooKeeper zooKeeper, OnElectionCallback onElectionCallback){
        this.zooKeeper = zooKeeper;
        this.onElectionCallback = onElectionCallback;
    }



    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL ); //ephemeral znode

        System.out.println("znode name: " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void reelectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;
        String predecessorZnodeName = "";
        while(predecessorStat == null){
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

            Collections.sort(children);
            String smallestChildren = children.get(0);

            if(smallestChildren.equals(currentZnodeName)){
                System.out.println("I am the leader");
                onElectionCallback.onElectedToBeLeader();
                return;
            }else{
                System.out.println("I am not the leader");
                int predecessor = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessor);
                predecessorStat =zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }

        onElectionCallback.onWorker();

        System.out.println("Watching znode  " +predecessorZnodeName);
        System.out.println();

    }



    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()){
//            case None:
//                if(event.getState() == Event.KeeperState.SyncConnected){ //successfully sync zookeeper server
//                    System.out.println("Successfully connect to Zookeeper");
//                }else{
//                    synchronized (zooKeeper){
//                        System.out.println("Disconnected from Zookeeper event");
//                        zooKeeper.notifyAll();
//                    }
//                }
//                break;
            case NodeDeleted:
                try{
                    reelectLeader();
                } catch (InterruptedException e) {
                } catch (KeeperException e) {
                }
        }
    }
}
