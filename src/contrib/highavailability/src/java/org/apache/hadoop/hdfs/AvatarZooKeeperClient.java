package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

public class AvatarZooKeeperClient {
  private String connection;
  private int timeout;
  private boolean watch;
  // Prefix under which the data for this client will be stored
  private String prefix;
  private Watcher watcher;
  private ZooKeeper zk;

  // Making it large enough to be sure that the cluster is down
  // these retries go one after another so they do not take long
  public static final int ZK_CONNECTION_RETRIES = 100;
  
  public AvatarZooKeeperClient(Configuration conf, Watcher watcher) {
    this.connection = conf.get("fs.ha.zookeeper.quorum");
    this.timeout = conf.getInt("fs.ha.zookeeper.timeout", 3000);
    this.watch = conf.getBoolean("fs.ha.zookeeper.watch", false);
    this.prefix = conf.get("fs.ha.zookeeper.prefix", "/hdfs");
    if (watcher == null) {
      // If there was no watcher regardless of the watch policy in the conf
      // set it to false. Since there is no watcher being set
      watch = false;
    }
    if (watch) {
      this.watcher = watcher;
    } else {
      this.watcher = (new Watcher() {

        @Override
        public void process(WatchedEvent event) {
          // This is a stub for ZK compatibility
          // if it is not there ZK will keep writing errors to the log
        }
      });
    }
  }
  
  public void clearPrimary(String address) throws IOException {
    String node = getRegistrationNode(address);
    zkCreateRecursively(node, null);
  }
  
  public void registerPrimary(String address, String realAddress) 
    throws UnsupportedEncodingException, IOException {
    String node = getRegistrationNode(address);
    zkCreateRecursively(node, realAddress.getBytes("UTF-8"));
  }
  
  private void zkCreateRecursively(String zNode, byte[] data) throws IOException {
    initZK();
    System.out.println("create " + zNode);
    String[] parts = zNode.split("/");
    String path = "";
    byte[] payLoad = new byte[0];
    List<ACL> acls = new ArrayList<ACL>(1);
    acls.add(new ACL(Perms.ALL, new Id("world", "anyone")));
    
    try {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].isEmpty())
          continue;
        path += "/" + parts[i];
        if (i == parts.length - 1) {
          payLoad = data;
        }
        Stat stat;
        boolean created = false;
        while (!created) {
          // While loop to keep trying through the ConnectionLoss exceptions
          try {
            if ((stat = zk.exists(path, false)) != null) {
              // -1 indicates that we should update zNode regardless of its
              // version
              // since we are not utilizing versions in zNode - this is the best
              zk.setData(path, payLoad, -1);
            } else {
              zk.create(path, payLoad, acls, CreateMode.PERSISTENT);
            }
            created = true;
          } catch (KeeperException ex) {
            ex.printStackTrace();
            if (KeeperException.Code.CONNECTIONLOSS != ex.code()) {
              throw ex;
            }
          }
        }
      }
      FileSystem.LOG.info("Wrote zNode " + zNode);
    } catch (KeeperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    } finally {
      try {
        stopZK();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Tries to connect to ZooKeeper. To be used when we need to test if the
   * ZooKeeper cluster is available and the config is correct
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public synchronized void primeConnection() throws IOException,
      InterruptedException {
    initZK();
    if (!watch) {
      stopZK();
    }
  }

  private void initZK() throws IOException {
    if (zk == null)
      zk = new ZooKeeper(connection, timeout, watcher);
  }

  private void stopZK() throws InterruptedException {
    if (zk == null)
      return;
    zk.close();
    zk = null;
  }

  /**
   * Get the information stored in the node of zookeeper. If retry is set
   * to true it will keep retrying until the data in that node is available
   * (failover case). If the retry is set to false it will return the first
   * value that it gets from the zookeeper.
   * 
   * @param node the path of zNode in zookeeper
   * @param stat {@link Stat} object that will contain stats of the node
   * @param retry if true will retry until the data in znode is not null
   * @return byte[] the data in the znode
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  private synchronized byte[] getNodeData(String node, Stat stat, boolean retry)
      throws IOException, KeeperException, InterruptedException {
    int failures = 0;
    
    byte[] data = null;
    while (data == null) {
      initZK();
      try {
        data = zk.getData(node, watch, stat);
        if (!watch) {
          stopZK();
        }
        if (data == null && retry) {
          // Failover is in progress
          // reset the failures
          failures = 0;
          DistributedAvatarFileSystem.LOG.info("Failover is in progress. Waiting");
          try {
            Thread.sleep(DistributedAvatarFileSystem.FAILOVER_CHECK_PERIOD);
          } catch (InterruptedException iex) {
            Thread.currentThread().interrupt();
          }
        } else {
          return data;
        }
      } catch (KeeperException kex) {
        if (KeeperException.Code.CONNECTIONLOSS == kex.code()
            && failures < ZK_CONNECTION_RETRIES) {
          failures++;
          // This means there was a failure connecting to zookeeper
          // we should retry since some nodes might be down.
          continue;
        }
        throw kex;
      }
    }
    return data;
  }

  public String getPrimaryAvatarAddress(URI address, Stat stat, boolean retry) 
    throws IOException, KeeperException, InterruptedException {
    String node = getRegistrationNode(address.getAuthority());
    byte[] data = getNodeData(node, stat, retry);
    if (data == null) {
      return null;
    }
    return new String(data, "UTF-8");
  }

  /**
   * Gets the {@link Stat} of the node. Will create and destroy connection if
   * the AvatarZooKeeperClient is not configured to set watchers
   * 
   * @param node
   *          the path of zNode to get {@link Stat} for
   * @return {@link Stat} of the node
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  private synchronized Stat getNodeStats(String node) throws IOException,
      KeeperException, InterruptedException {
    int failures = 0;
    boolean gotStats = false;
    Stat res = null;
    while (!gotStats) {
      initZK();
      try {
        res = zk.exists(node, watch);
        // Since stats can be null we have to control the execution with a flag
        gotStats = true;
        if (!watch) {
          stopZK();
        }
      } catch (KeeperException kex) {
        if (KeeperException.Code.CONNECTIONLOSS == kex.code()
            && failures < ZK_CONNECTION_RETRIES) {
          failures++;
          continue;
        }
        throw kex;
      }
    }
    return res;
  }

  public long getPrimaryRegistrationTime(URI address) throws IOException,
      KeeperException, InterruptedException {
    String node = getRegistrationNode(address.getAuthority());
    return getNodeStats(node).getMtime();
  }
  /*
   * ZNode address is formed by the logical name of the filesystem:
   * dfs.data.xxx.com:9000 will be represented by zNode
   * /prefix/dfs.data.xxx.com/9000 in ZooKeeper
   */
  private String getRegistrationNode(String clusterAddress) {
    return prefix + "/" + clusterAddress.replaceAll("[:]", "/");
  }
  public synchronized void shutdown() throws InterruptedException {
    stopZK();
  }
}
