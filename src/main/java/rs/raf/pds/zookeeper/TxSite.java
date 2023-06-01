package rs.raf.pds.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import rs.raf.pds.zookeeper.core.SyncPrimitive;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TxSite extends SyncPrimitive {
    public int site_id;
    public int current_id;
    public String node;
    public boolean isCoordStart;
    public static long TX_ABORT_RATE = 20L;

    protected TxSite(String address, String root, int id) {
        super(address);
        this.root = root;
        this.site_id = id;
        this.node = null;
        this.current_id = -1;
        this.isCoordStart = false;
        this.initialize();
    }
    //izvrsice se pri pokretanju TxSite-a
    //dohvataju se i prate informacije o poslednjoj trans iz txCoordinator-a
    private void initialize() {
        try {
            Stat s = this.zk.exists(this.root, true);
            if (s == null) {
                System.err.println("Transakcija nije startovana");
            }
            else {
                this.isCoordStart = true;
            }
        }
        catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    protected int getLastTxNumber(List<String> lista) {
        int max = Integer.MIN_VALUE;
        for (String s : lista) {
            int lastTx = Integer.parseInt(s.substring(s.indexOf("/tx") + "/tx".length()));
            if (max < lastTx) {
                max = lastTx;
            }
        }
        return max;
    }

    //gleda se koja će se odluka poslati koordinatoru
    //koristi se putanja cvor potomka kako bismo upisali vrednost ABORT/COMMIT u ZooKeeper cvor
    TxCoordinator.TransResult sendDecisionToCoordinator() throws InterruptedException, KeeperException {
        System.out.println("Slanje odluke koordinatoru");

        TxCoordinator.TransResult result = TxCoordinator.TransResult.NO_DECISION;
        while (result == TxCoordinator.TransResult.NO_DECISION) {
            synchronized (this.mutex) {
                this.mutex.wait();
                result = this.executeFinalDecision();
            }
        }
        return result;
    }
    // salje konacnu odluku koordinatoru
    TxCoordinator.TransResult executeFinalDecision() throws KeeperException, InterruptedException {
        byte[] b = this.zk.getData(this.root + this.node, true, null);
        if (b.length == 0) {
            return TxCoordinator.TransResult.NO_DECISION;
        }
        String decision = new String(b);
        System.out.println("String = " + decision);
        if (TxCoordinator.TransResult.ABORT.toString().equals(decision)) {
            return TxCoordinator.TransResult.ABORT;
        }
        if (TxCoordinator.TransResult.COMMIT.toString().equals(decision)) {
            return TxCoordinator.TransResult.COMMIT;
        }
        return TxCoordinator.TransResult.NO_DECISION;
    }

    //donosi odluku koloko je transakcija bila uspesna
    //gledamo da li je trans uspesna ili ne
    public void makeDecision() throws KeeperException, InterruptedException {
        List<String> lista;
        while (true) {
            lista = this.zk.getChildren(this.root, true);
            System.out.println("Imamo " + lista.size() + " potomaka od tx root-a");
            int maxId = -1;
            if (!lista.isEmpty()) {
                maxId = this.getLastTxNumber(lista);
            }
            if (lista.isEmpty() || this.current_id == maxId) {
                synchronized (this.mutex) {
                    this.mutex.wait();
                }
            }else {
                break;
            }
        }
        this.current_id = this.getLastTxNumber(lista);
        this.node = "/tx" + this.current_id;
        String myTxNode = this.node + "/s" + this.site_id;
        long time = ThreadLocalRandom.current().nextLong(0, 100);
        Thread.sleep(100);
        String decision = (time <= TxSite.TX_ABORT_RATE)
                ? TxCoordinator.TransResult.ABORT.toString()
                : TxCoordinator.TransResult.COMMIT.toString();
        System.out.println("Tx site " + this.site_id + " ima odluku:" + decision);
        this.zk.setData(this.root + myTxNode, decision.getBytes(), -1);
    }

    public static void main(String[] args) throws InterruptedException {
        int id = Integer.parseInt(args[1]);
        TxSite site = new TxSite(args[0], "/trans", id);
        while (!site.isCoordStart) {
            Thread.sleep(ThreadLocalRandom.current().nextLong(500, 1000));
            site.initialize();
        }
        while (true) {
            try {
                site.makeDecision();
                TxCoordinator.TransResult result = site.executeFinalDecision();
                if (result != TxCoordinator.TransResult.NO_DECISION) {
                    System.out.println("Treba jos sacekati na odluku koordinatora: " + result.toString());
                } else {
                    result = site.sendDecisionToCoordinator();
                    System.out.println("Odluka koordinatora: " + result.toString());
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
    }
}
