/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.util.GSet;
import org.apache.hadoop.hdfs.util.LightWeightGSet;

/**
 * Manage node decommissioning.
 */
class DecommissionManager {
  static final Log LOG = LogFactory.getLog(DecommissionManager.class);

  private final FSNamesystem fsnamesystem;

  DecommissionManager(FSNamesystem namesystem) {
    this.fsnamesystem = namesystem;
  }

  /** Periodically check decommission status. */
  class Monitor implements Runnable {
    /** recheckInterval is how often namenode checks
     *  if a node has finished decommission
     */
    private final long recheckInterval;
    /** The number of decommission nodes to check for each interval */
    private final int numNodesPerCheck;

    // datanodes that just started decomission,
    // which has higher priority to be checked next
    private final LinkedList<DatanodeDescriptor> newlyStarted =
      new LinkedList<DatanodeDescriptor>();
    // datanodes that needs to be checked next
    private LinkedList<DatanodeDescriptor> toBeChecked =
      new LinkedList<DatanodeDescriptor>();
    // datanodes that just finished check
    private LinkedList<DatanodeDescriptor> checked =
      new LinkedList<DatanodeDescriptor>();

    // the node is under check
    private volatile DatanodeDescriptor nodeBeingCheck;
    // if there was an attempt to stop nodeBeingCheck from decommission
    private volatile boolean pendingToStopDecommission = false;
    
    Monitor(int recheckIntervalInSecond, int numNodesPerCheck) {
      this.recheckInterval = recheckIntervalInSecond * 1000L;
      this.numNodesPerCheck = numNodesPerCheck;
    }

    /**
     * Add a datanode that is just marked to start decommission
     * @param datanode a newly marked decommissioned node
     * @return true if the node is added
     */
    synchronized boolean startDecommision(DatanodeDescriptor datanode) {
      if (datanode == null) {
        throw new IllegalArgumentException(
            "datanode to be decomissioned can not be null");
      }
      if (nodeBeingCheck == datanode) {
        pendingToStopDecommission = false;
        return false;
      } 
      if (!newlyStarted.contains(datanode) && 
          !toBeChecked.contains(datanode) && !checked.contains(datanode)) {
        newlyStarted.offer(datanode);
        notifyAll();
        return true;
      }
      return false;
    }
    
    /**
     * Stop a node from decommission by removing it from the queue
     * @param datanode a datanode
     * @return true if decommission is stopped; false if it is pending
     */
    synchronized boolean stopDecommission(DatanodeDescriptor datanode)
    throws IOException {
      if (datanode == null) {
        throw new IllegalArgumentException(
        "datanode to be removed can not be null");
      }
      if (datanode == nodeBeingCheck) {
        // the node to be stopped decommission is under check
        // so waiting for it to be done
        pendingToStopDecommission = true;
        return false;
      }
      if (newlyStarted.remove(datanode) ||
               toBeChecked.remove(datanode)) {
        checked.remove(datanode);
      }
      return true;
    }
    
    /**
     * Return a list of unchecked blocks on srcNode
     * 
     * @param srcNode a datanode
     * @param checkedBlocks all blocks that have been checked
     * @param numBlocks maximum number of blocks to return
     * @return a list of blocks to be checked
     */
    private List<Block> fetchBlocks(
        GSet<Block, Block> checkedBlocks, int numBlocks) {
      final List<Block> blocksToCheck = new ArrayList<Block>(numBlocks);
      fsnamesystem.readLock();
      try {
        final Iterator<Block> it = nodeBeingCheck.getBlockIterator();
        while (blocksToCheck.size()<numBlocks && it.hasNext()) {
          final Block block = it.next();
          if (!checkedBlocks.contains(block)) { // the block has not been checked
            blocksToCheck.add(block);
          }
        }
      } finally {
        fsnamesystem.readUnlock();
      }
      return blocksToCheck;
    }
    
    synchronized private void handlePendingStopDecommission() {
      if (pendingToStopDecommission) {
        LOG.info("Stop (delayed) Decommissioning node " + 
            nodeBeingCheck.getName());
        nodeBeingCheck.stopDecommission();
        pendingToStopDecommission = false;
      }
    }

    /**
     * Change, if appropriate, the admin state of a datanode to 
     * decommission completed. Return true if decommission is complete.
     */
    private boolean checkDecommissionStateInternal() {
      fsnamesystem.writeLock();
      int numOfBlocks;
      try {
        if (!nodeBeingCheck.isDecommissionInProgress()) {
          return true;
        }
        // initialize decominssioning status
        nodeBeingCheck.decommissioningStatus.set(0, 0, 0);
        numOfBlocks = nodeBeingCheck.numBlocks();
      } finally {
        fsnamesystem.writeUnlock();
      }
      
      //
      // Check to see if all blocks in this decommissioned
      // node has reached their target replication factor.
      //
      // limit the number of scans
      final int BLOCKS_PER_ITER = 1000;
      final int numOfBlocksToFetch = Math.max(BLOCKS_PER_ITER, numOfBlocks/5);
      GSet<Block, Block> checkedBlocks =
        new LightWeightGSet<Block, Block>(numOfBlocks);
      List<Block> blocksToCheck;
      int numBlocksToCheck;
      do {
        // get a batch of unchecked blocks
        blocksToCheck = fetchBlocks(checkedBlocks, numOfBlocksToFetch);
        numBlocksToCheck = blocksToCheck.size();
        for (int i=0; i<numBlocksToCheck; ) {
          fsnamesystem.writeLock();
          try {
            for (int j=0; j<BLOCKS_PER_ITER && i<numBlocksToCheck; j++, i++) {
              // check if each block reaches its replication factor or not
              Block blk = blocksToCheck.get(i);
              fsnamesystem.isReplicationInProgress(nodeBeingCheck, blk);
              checkedBlocks.put(blk);
            }
          } finally {
            fsnamesystem.writeUnlock();
          }
        }
      } while (numBlocksToCheck != 0);
        
      fsnamesystem.writeLock();
      try {
        handlePendingStopDecommission();
        if (!nodeBeingCheck.isDecommissionInProgress()) {
          return true;
        }
        if (nodeBeingCheck.decommissioningStatus.
            getUnderReplicatedBlocks() == 0) {
         nodeBeingCheck.setDecommissioned();
         LOG.info("Decommission complete for node " + nodeBeingCheck.getName());
         return true;
        }
      } finally {
        fsnamesystem.writeUnlock();
      }
      
      return false;
    }

    /**
     * Wait for more work to do
     * @return true if more work to do; false if gets interrupted
     */
    synchronized private boolean waitForWork() {
      try {
        if (newlyStarted.isEmpty() && toBeChecked.isEmpty()) {
          do {
            wait();
          } while (newlyStarted.isEmpty() && toBeChecked.isEmpty());
        } else {
          Thread.sleep(recheckInterval);
        }
        return true;
      } catch (InterruptedException ie) {
        LOG.info("Interrupted " + this.getClass().getSimpleName(), ie);
        return false;
      }
    }
    
    /**
     * Check decommission status of numNodesPerCheck nodes;
     * sleep if there is no decommission node;
     * otherwise wakeup for every recheckInterval milliseconds.
     */
    public void run() {
      while (fsnamesystem.isRunning() && !Thread.interrupted()) {
        try {
          if (waitForWork()) {
            check();
          } else {
            break;
          }
        } catch (Exception e) {
          LOG.warn("DecommissionManager encounters an error: ", e);
        }
      }
    }
    
    /**
     * Get the next datanode that's decommission in progress
     */
    synchronized private void getDecommissionInProgressNode() {
      nodeBeingCheck = newlyStarted.poll();
      if (nodeBeingCheck == null) {
        nodeBeingCheck = toBeChecked.poll();
      }
      
      if (nodeBeingCheck == null) {
        // all datanodes have been checked; preparing for the next iteration
        LinkedList<DatanodeDescriptor> tmp = toBeChecked;
        toBeChecked = checked;
        checked = tmp;
      }
    }
    
    /**
     * Mark the given datanode as just checked
     * @param datanode
     */
    synchronized private void doneCheck(final boolean isDecommissioned) {
      if (!isDecommissioned) {
        // put to checked for next iteration of check
        checked.add(nodeBeingCheck);
      }
      nodeBeingCheck = null;
    }
    
    /**
     * Check up to numNodesPerCheck decommissioning in progress datanodes to
     * see if all their blocks are replicated.
     */
    private void check() {
      for (int i=0; i<numNodesPerCheck; i++) {
        getDecommissionInProgressNode();
        if (nodeBeingCheck == null) {
          break;
        }
        try {
          boolean isDecommissioned =
            checkDecommissionStateInternal();
          doneCheck(isDecommissioned);
        } catch(Exception e) {
          LOG.warn("entry=" + nodeBeingCheck, e);
        }
      }
    }
  }
}
