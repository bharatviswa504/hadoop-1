/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.ratis;

import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.ratis.helpers.DoubleBufferEntry;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.utils.db.BatchOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A double-buffer for OM requests.
 */
public class OzoneManagerDoubleBuffer {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerDoubleBuffer.class.getName());

  private TransactionBuffer currentBuffer;
  private TransactionBuffer readyBuffer;
  private Daemon daemon;
  private volatile boolean syncToDB;
  private final OMMetadataManager omMetadataManager;
  private AtomicLong flushedTransactionCount = new AtomicLong(0);
  private AtomicLong flushIterations = new AtomicLong(0);

  public OzoneManagerDoubleBuffer(OMMetadataManager omMetadataManager) {
    this.currentBuffer = new TransactionBuffer();
    this.readyBuffer = new TransactionBuffer();
    this.omMetadataManager = omMetadataManager;

    // Daemon thread which runs in back ground and flushes transactions to DB.
    daemon = new Daemon(this::flushTransactions);
    daemon.start();

  }

  /**
   * Runs in a background thread and batches the transaction in currentBuffer
   * and commit to DB.
   */
  private void flushTransactions() {
    while(true) {
      if (canFlush()) {
        syncToDB = true;
        setReadyBuffer();
        final BatchOperation batchOperation = omMetadataManager.getStore()
            .initBatchOperation();

        readyBuffer.iterator().forEachRemaining((entry) -> {
          try {
            entry.getResponse().addToRocksDBBatch(omMetadataManager,
                batchOperation);
          } catch (IOException ex) {
            // During Adding to RocksDB batch entry got an exception.
            // We should terminate the OM.
            String message = "During flush to DB encountered error " +
                ex.getMessage();
            ExitUtil.terminate(1, message);
          }
        });

        try {
          omMetadataManager.getStore().commitBatchOperation(batchOperation);
        } catch (IOException ex) {
          // During flush to rocksdb got an exception.
          // We should terminate the OM.
          String message = "During flush to DB encountered error " +
              ex.getMessage();
          ExitUtil.terminate(1, message);
        }

        int flushedTransactionsSize = readyBuffer.size();
        flushedTransactionCount.addAndGet(flushedTransactionsSize);
        flushIterations.incrementAndGet();

        LOG.info("Sync Iteration {} flushed transactions in this iteration{}",
            flushIterations.get(), flushedTransactionsSize);
        readyBuffer.clear();
        syncToDB = false;
        // TODO: update the last updated index in OzoneManagerStateMachine.
      }
    }
  }

  /**
   * Returns the flushed transaction count to OM DB.
   * @return flushedTransactionCount
   */
  public long getFlushedTransactionCount() {
    return flushedTransactionCount.get();
  }

  /**
   * Returns total number of flush iterations run by sync thread.
   * @return flushIterations
   */
  public long getFlushIterations() {
    return flushIterations.get();
  }

  /**
   * Add OmResponseBufferEntry to buffer.
   * @param response
   * @param transactionIndex
   */
  public synchronized void add(OMClientResponse response,
      long transactionIndex) {
    currentBuffer.add(new DoubleBufferEntry<>(transactionIndex, response));
  }

  /**
   * Check can we flush transactions or not.
   *
   * @return boolean
   */
  @SuppressFBWarnings(value="IS2_INCONSISTENT_SYNC", justification =
      "Just checking size here, so we don't need synchronize here")
  private boolean canFlush() {
    return !syncToDB && currentBuffer.size() > 0;
  }

  /**
   * Prepares the readyBuffer which is used by sync thread to flush
   * transactions to OM DB.
   */
  private synchronized void setReadyBuffer() {
    TransactionBuffer temp = currentBuffer;
    currentBuffer = readyBuffer;
    readyBuffer = temp;
  }

  /**
   * TransactionBuffer which holds queue of
   * {@link org.apache.hadoop.ozone.om.ratis.helpers.DoubleBufferEntry}.
   */
  static class TransactionBuffer {

    private Queue<DoubleBufferEntry<OMClientResponse>> queue;

    TransactionBuffer() {
      // Taken unbounded queue, if sync thread is taking too long time, we
      // might end up taking huge memory to add entries to the buffer.
      // TODO: We can avoid this using unbounded queue and use queue with
      // capcity, if queue is full we can wait for sync to be completed to
      // add entries. But in this also we might block rpc handlers, as we
      // clear entries after sync. Or we can come up with a good approach to
      // solve this.
      queue = new ConcurrentLinkedQueue<>();
    }

    /**
     * Add the entry to the queue.
     * @param responseDoubleBufferEntry
     */
    void add(DoubleBufferEntry<OMClientResponse> responseDoubleBufferEntry) {
      queue.add(responseDoubleBufferEntry);
    }

    /**
     * Returns the size of the TransactionBuffer.
     * @return
     */
    int size() {
      return queue.size();
    }

    /**
     * Clear entries in the buffer.
     */
    void clear() {
      queue.clear();
    }

    /**
     * Returns an iterator.
     * @return Iterator<DoubleBufferEntry<OmResponseBufferEntry>>
     */
    Iterator<DoubleBufferEntry<OMClientResponse>> iterator() {
      return queue.iterator();
    }

  }
}
