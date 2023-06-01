package rs.raf.pds.zookeeper;


import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import rs.raf.pds.zookeeper.core.SyncPrimitive;

public class LeaderElection extends SyncPrimitive implements Runnable{
    
	public static final String ROOT_NODE_NAME ="/elect";
	public static final String REPLICA_NODE_NAME ="/candid";
	public static final int REPLICA_NODE_SEQUENCE_INDEX= REPLICA_NODE_NAME.length()-1; 
	public enum Role {FOLLOWER, LEADER};
	
	int myId = -1;
	volatile Role myRole = Role.FOLLOWER;
	volatile boolean execute = true;
	 
	protected LeaderElection(String address, String root) {
		super(address);
		this.root = root;
		
		// Create election node
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
                
               String myNodeName = zk.create(root + REPLICA_NODE_NAME, new byte[0], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
               
               System.out.println("My Node election name:"+myNodeName);
               int tempIndex = myNodeName.indexOf(REPLICA_NODE_NAME)+REPLICA_NODE_NAME.length();
               this.myId = Integer.parseInt(myNodeName.substring(tempIndex));
               
               System.out.println("Replica election ID = "+myId);
                
            } catch (KeeperException e) {
                System.out
                        .println("Keeper exception when instantiating queue: "
                                + e.toString());
            } catch (InterruptedException e) {
                System.out.println("Interrupted exception");
            }
        }

    }
	protected void setLeader() {
		myRole = Role.LEADER;
		System.out.println("JA SAM LIDER!");
		
	}
	
	private void checkReplicaCandidate() throws KeeperException, InterruptedException {
		List<String> list = zk.getChildren(root, false);
        System.out.println("There are total:"+list.size()+ " replicas for elections!");
        for (int i=0; i<list.size(); i++) 
        	System.out.print("NODE:"+list.get(i)+", ");
        System.out.println();
        
        if (list.size() == 0) {
            System.out.println("0 Elemenata ? A ja ??? ");
           // mutex.wait();
        } else {
            Collections.sort(list);
            int myIndex = -1;
            
            for(int i=0; i<list.size(); i++) {
            	Integer tempValue = Integer.parseInt(list.get(i).substring(REPLICA_NODE_SEQUENCE_INDEX));
                if(myId == tempValue) {
                   myIndex = i;
                   break;
                }
            }
            if (myIndex == 0) {
            	setLeader();
            }
            else {
            	String myLeaderNodeToWatch = list.get(myIndex-1);
            	// byte[] b = zk.getData(root + "/" + myLeaderNodeToWatch, true, null);
				Stat stat = zk.exists(root + "/" + myLeaderNodeToWatch, true);
            	if (stat == null)
            		setLeader();
            }
        }
	}
	public void election() throws KeeperException, InterruptedException {
		checkReplicaCandidate();
	}
	@Override
	public void run() {
		while(execute) {
			try {
				if (myRole==Role.LEADER) {
					System.out.println("Izvrsavam Leader Task!");
				}
				else {
					System.out.println("Izvrsavam Follower Task!");
				}
				Thread.sleep(ThreadLocalRandom.current().nextLong(5000, 10000));
				
			}catch(InterruptedException e) {
				
			}
		}
		
	}
	public void waitForChange(Thread workingThread) throws InterruptedException, KeeperException {
		while(true) {
			synchronized(mutex) {
			   mutex.wait();
			   System.out.println("Stigla notifikacija");
			}
			checkReplicaCandidate();
			workingThread.interrupt();
		}
	}
	
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Usage java -cp PDS-Zookeeper2-1.0.jar;.;lib/* rs.raf.pds.zookeeper.LeaderElection <zookeeper_server_host:port>");
		}
		
		LeaderElection replika = new LeaderElection(args[0], ROOT_NODE_NAME);
		
		 try{
	        replika.election();
	        Thread workingThread = new Thread(replika);
	        workingThread.start();
	        
	        replika.waitForChange(workingThread);
	          
	     } catch (KeeperException e){

	     } catch (InterruptedException e){

	     }
		
    }
	
}