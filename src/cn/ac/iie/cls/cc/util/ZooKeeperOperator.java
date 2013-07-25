/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.util;


import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 *
 * @author root
 */
public class ZooKeeperOperator extends AbstractZooKeeper {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ZooKeeperOperator.class);

    public void create(String path, byte[] data, String mode) throws KeeperException, InterruptedException {
        //this.zooKeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT/*此处创建的为持久态的节点,可为瞬态*/); 
        if (mode.equals("PERSISTENT")) {
            this.zooKeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT/*
                     * 此处创建的为持久态的节点,可为瞬态
                     */);
        } else if (mode.equals("EPHEMERAL")) {
            this.zooKeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL/*
                     * 此处创建的为持久态的节点,可为瞬态
                     */);
        }

    }

    public String exists(String path) {
        try {
            Stat s = zooKeeper.exists(path, false);
            if (s != null) {
                return "true";
            } else {
                return "false";
            }
        } catch (Exception ex) {

            logger.debug("ZooKeeperOperator exists err!" + "|"+ex.getLocalizedMessage() + "|" + ex.getMessage() + "|" + ex.getStackTrace());
            return "err";
        }
    }

    /**
     * 获取节点的孩子信息
     *
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void getChild(String path) throws KeeperException, InterruptedException {
        try {
            List<String> children = this.zooKeeper.getChildren(path, false);
            if (children.isEmpty()) {
                logger.debug("没有节点在%s中."+path);
                return;
            } else {
                logger.debug("节点%s中存在的节点:\n"+path);
                for (String child : children) {
                    logger.debug(child);
                }
            }
        } catch (KeeperException.NoNodeException e) {
            logger.debug("%s节点不存在."+ path);
            throw e;
        }
    }

    public byte[] getData(String path) throws KeeperException, InterruptedException {
        return this.zooKeeper.getData(path, false, null);
    }
    /*
     * public static void main(String[] args) { try { ZooKeeperOperator
     * zkoperator = new ZooKeeperOperator();
     * zkoperator.connect("192.168.111.128"); byte[] data = new
     * byte[]{'d','a','t','a'};
     *
     * zkoperator.create("/root",null);
     * System.out.println(Arrays.toString(zkoperator.getData("/root")));
     *
     * zkoperator.create("/root/child1",data);
     * System.out.println(Arrays.toString(zkoperator.getData("/root/child1")));
     *
     * zkoperator.create("/root/child2",data);
     * System.out.println(Arrays.toString(zkoperator.getData("/root/child2")));
     *
     * System.out.println("节点孩子信息:"); zkoperator.getChild("/root");
     *
     * zkoperator.setWatcher();
     *
     * while(true){ Thread.sleep(2000); } //zkoperator.close(); } catch
     * (Exception e) { e.printStackTrace(); }
	}
     */
}
