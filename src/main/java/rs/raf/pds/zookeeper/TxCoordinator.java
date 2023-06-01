package rs.raf.pds.zookeeper;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import rs.raf.pds.zookeeper.core.SyncPrimitive;

public class TxCoordinator extends SyncPrimitive{

	
	public static final String ROOT_NODE_NAME ="/trans";
	public static final String TX_NODE_NAME ="/tx";
	public static enum TransResult {NO_DECISION, COMMIT, ABORT};
	
	AtomicInteger transCounter = new AtomicInteger(0);
	int sites = 0;
	String currentTransNodeName = null;
	TransResult finalTxDecision = TransResult.NO_DECISION;
	
	AtomicInteger siteAcksCounter = null;
	BlockingQueue<WatchedEvent> siteAcks = new LinkedBlockingQueue<>();
	
	
	protected TxCoordinator(String address, String root, int sites) {
		super(address);
		this.root = root;
		this.sites = sites;
		
		// Create transaction node
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s != null) { 
                	int lastTransId = getLastTransId();
                	System.out.println("Last transaction id ="+lastTransId);
                	transCounter = new AtomicInteger(lastTransId);                	
            	}
                zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
                     CreateMode.PERSISTENT);
                   
            } catch (KeeperException e) {
            System.out
                    .println("Keeper exception: "
                            + e.toString());
	        } catch (InterruptedException e) {
	            System.out.println("Interrupted exception");
	        }
       }
	}
	protected int getLastTransId() throws KeeperException, InterruptedException {
		List<String> transList = zk.getChildren(root, true);
		int maxTxId = 0;
		if (transList.size()>0) 
			maxTxId = getLastTxNumber(transList);
		return maxTxId;
	}
	protected int getLastTxNumber(List<String> list) {
		int max = Integer.MIN_VALUE;
		for (String s:list) {
			int lastTx = Integer.parseInt(s.substring(s.indexOf(TxCoordinator.TX_NODE_NAME)+TxCoordinator.TX_NODE_NAME.length())); 
			if (max<lastTx)
				max=lastTx;
		}
		return max;
	}
	public void startTransaction() throws KeeperException, InterruptedException {
		finalTxDecision = TransResult.NO_DECISION;
		String transName = TX_NODE_NAME+transCounter.addAndGet(1);
		currentTransNodeName = zk.create(root + transName, new byte[0], Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		
		for (int i=0; i<sites; i++) {
			String siteNodeName = zk.create(currentTransNodeName + "/s"+i, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL);
			byte[] b = zk.getData(siteNodeName, true, null);
		}
		
		siteAcksCounter = new AtomicInteger(sites);
		
		System.out.println("Transaction "+transName+" started!");
	}
	public TransResult awaitTransactionEnd() throws InterruptedException, KeeperException {
		while(true) {
			WatchedEvent event = siteAcks.take();
			TransResult siteTxDecision = checkTransResult(event);
			if (siteTxDecision == TransResult.ABORT) {
				finalTxDecision = TransResult.ABORT;
					//break;
			}
			if (siteAcksCounter.decrementAndGet() == 0)
				break;
		}
		if (finalTxDecision == finalTxDecision.NO_DECISION)
			finalTxDecision = TransResult.COMMIT;
		
		
		return finalTxDecision;
		
	}
	public void setFinalDecision() throws KeeperException, InterruptedException {
		zk.setData(currentTransNodeName, finalTxDecision.toString().getBytes(), -1);
	}
	protected TransResult checkTransResult(WatchedEvent event) throws KeeperException, InterruptedException {
		String nodePath = event.getPath();
        TransResult siteTxResult = TransResult.NO_DECISION;
		byte[] data;
		
		data = zk.getData(nodePath, false, null);
		String reply = new String(data);
	    System.out.println("Rezultat site-a:"+reply);
	    if (TransResult.ABORT.toString().equals(reply))
	    	siteTxResult = TransResult.ABORT;
	    else if (TransResult.COMMIT.toString().equals(reply))
	    	siteTxResult = TransResult.COMMIT;
	
	    return siteTxResult;
	}
	@Override
	synchronized public void process(WatchedEvent event) {
        
		if (event.getPath() != null)
			siteAcks.add(event);
//		try {
//			TransResult siteTxDecision = checkTransResult(event);
//		} catch (KeeperException | InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		synchronized (mutex) {
//            //System.out.println("Process: " + event.getType());
//            mutex.notify();
//        }

    }
	
	public void cleanTransactionData() throws InterruptedException, KeeperException {
		zk.delete(currentTransNodeName, -1);
	}
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Usage java -jar pds-zookeeper2.jar <zookeeper_server_host:port> <sites>");
		}
		Integer sites = Integer.parseInt(args[1]);
		TxCoordinator coordinator = new TxCoordinator(args[0], ROOT_NODE_NAME, sites);
		while(true) {
			 try{
		        
				coordinator.startTransaction();
		        TransResult result = coordinator.awaitTransactionEnd();
		        
		        if (result == TransResult.ABORT) {
		        	System.out.println("Transakcija je prekinuta!");
		        }else if (result == TransResult.COMMIT) {
		        	System.out.println("Transakcija je commitovana!");
		        }
		        coordinator.setFinalDecision();
		        
		        Thread.sleep(ThreadLocalRandom.current().nextLong(1000, 2000));
		        //coordinator.cleanTransactionData();
		        
		     } catch (KeeperException e){
		    	 e.printStackTrace();
		     } catch (InterruptedException e){
		     }
		}	
		
	}
		 
}
