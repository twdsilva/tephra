/*
 * Copyright © 2012-2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.tephra.coprocessor;

import co.cask.tephra.TxConstants;
import co.cask.tephra.metrics.TxMetricsCollector;
import co.cask.tephra.persist.HDFSTransactionStateStorage;
import co.cask.tephra.persist.MinimalTransactionSnapshot;
import co.cask.tephra.persist.TransactionStateStorage;
import co.cask.tephra.snapshot.SnapshotCodecProvider;
import co.cask.tephra.util.ConfigurationFactory;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Periodically refreshes transaction state from the latest stored snapshot.  This is implemented as a singleton
 * to allow a single cache to be shared by all regions on a regionserver.
 */
public class TransactionStateCache extends AbstractIdleService implements Configurable {
  private static final Log LOG = LogFactory.getLog(TransactionStateCache.class);

  // how frequently we should wake to check for changes (in seconds)
  private static final long CHECK_FREQUENCY = 15;

  private Configuration hConf;

  private TransactionStateStorage storage;
  private volatile MinimalTransactionSnapshot latestState;

  private Thread refreshService;
  private long lastRefresh;
  // snapshot refresh frequency in milliseconds
  private long snapshotRefreshFrequency;
  private boolean initialized;

  public TransactionStateCache() {
  }

  @Override
  public Configuration getConf() {
    return hConf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.hConf = conf;
  }

  @Override
  protected void startUp() throws Exception {
    refreshState();
    startRefreshService();
  }

  @Override
  protected void shutDown() throws Exception {
    this.refreshService.interrupt();
    this.storage.stop();
  }

  /**
   * Try to initialize the Configuration and TransactionStateStorage instances.  Obtaining the Configuration may
   * fail until ReactorServiceMain has been started.
   */
  private void tryInit() {
    try {
      Configuration conf = getSnapshotConfiguration();
      if (conf != null) {
        // Since this is only used for background loading of transaction snapshots, we use the no-op metrics collector,
        // as there are no relevant metrics to report
        this.storage = new HDFSTransactionStateStorage(conf, new SnapshotCodecProvider(conf),
                                                       new TxMetricsCollector());
        this.storage.startAndWait();
        this.snapshotRefreshFrequency = conf.getLong(TxConstants.Manager.CFG_TX_SNAPSHOT_INTERVAL,
                                                     TxConstants.Manager.DEFAULT_TX_SNAPSHOT_INTERVAL) * 1000;
        this.initialized = true;
      } else {
        LOG.info("Could not load configuration");
      }
    } catch (Exception e) {
      LOG.info("Failed to initialize TransactionStateCache due to: " + e.getMessage());
    }
  }

  protected Configuration getSnapshotConfiguration() throws IOException {
    Configuration conf = new ConfigurationFactory().get(hConf);
    conf.unset(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES);
    return conf;
  }

  private void reset() {
    this.storage.stop();
    this.lastRefresh = 0;
    this.initialized = false;
  }

  private void startRefreshService() {
    this.refreshService = new Thread("tx-state-refresh") {
      @Override
      public void run() {
        while (!isInterrupted()) {
          if (latestState == null || System.currentTimeMillis() > (lastRefresh + snapshotRefreshFrequency)) {
            try {
              refreshState();
            } catch (IOException ioe) {
              LOG.info("Error refreshing transaction state cache: " + ioe.getMessage());
            }
          }
          try {
            TimeUnit.SECONDS.sleep(CHECK_FREQUENCY);
          } catch (InterruptedException ie) {
            // reset status
            interrupt();
            break;
          }
        }
        LOG.info("Exiting thread " + getName());
      }
    };
    this.refreshService.setDaemon(true);
    this.refreshService.start();
  }

  private void refreshState() throws IOException {
    if (!initialized) {
      tryInit();
    }

    // only continue if initialization was successful
    if (initialized) {
      long now = System.currentTimeMillis();
      MinimalTransactionSnapshot currentSnapshot = storage.getLatestMinimalSnapshot();
      if (currentSnapshot != null) {
        if (currentSnapshot.getTimestamp() < (now - 2 * snapshotRefreshFrequency)) {
          LOG.info("Current snapshot is old, will force a refresh on next run.");
          reset();
        } else {
          latestState = currentSnapshot;
          LOG.info("Transaction state reloaded with snapshot from " + latestState.getTimestamp());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Latest transaction snapshot: " + latestState.toString());
          }
          lastRefresh = now;
        }
      } else {
        LOG.info("No transaction state found.");
      }
    }
  }

  public MinimalTransactionSnapshot getLatestState() {
    return latestState;
  }
}
