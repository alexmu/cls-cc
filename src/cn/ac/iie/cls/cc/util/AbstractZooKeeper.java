package cn.ac.iie.cls.cc.util;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

/**
 * @author ruodao
 * @since 1.0 2010-2-18 下午08:57:36
 */
public class AbstractZooKeeper implements Watcher {

    private static Logger logger = Logger.getLogger(AbstractZooKeeper.class);
    private static final int SESSION_TIME = 2000;
    protected ZooKeeper zooKeeper;
    protected CountDownLatch countDownLatch = new CountDownLatch(1);

    public void setWatcher(String path) throws KeeperException, InterruptedException {

        //Stat s = zooKeeper.exists(path, true);
        zooKeeper.getChildren(path, true);
        /*
         * if (s != null) { zooKeeper.getData(path, false, s);
        }
         */

    }

    void trigeWatcher() {
        try {
            Stat s = zooKeeper.exists("/root", false);        //此处不设置watcher
            Stat setData = zooKeeper.setData("/root", "a".getBytes(), s.getVersion()); //修改数据时需要提供version
        } catch (Exception ex) {
            logger.debug("AbstractZooKeeper trigeWatcher err!" + "|"+ex.getLocalizedMessage() + "|" + ex.getMessage() + "|" + ex.getStackTrace());
        }
    }

    public void connect(String hosts) throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(hosts, SESSION_TIME, this);
        countDownLatch.await();
    }

    public void process(WatchedEvent event) {
        try {
                    if (event.getState() == KeeperState.SyncConnected) {
                        countDownLatch.countDown();
                    }
            
            
        } catch (Exception ex) {
            //logger.debug("zooKeeper.getChildren err!" + "|"+ex.getLocalizedMessage() + "|" + ex.getMessage() + "|" + ex.getStackTrace());
        }
        
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}
