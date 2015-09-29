/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.tephra.distributed;

import co.cask.tephra.ThriftTransactionSystemTest;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.TxConstants;
import co.cask.tephra.persist.InMemoryTransactionStateStorage;
import co.cask.tephra.persist.TransactionEdit;
import co.cask.tephra.persist.TransactionLog;
import co.cask.tephra.persist.TransactionStateStorage;
import co.cask.tephra.runtime.ConfigModule;
import co.cask.tephra.runtime.DiscoveryModules;
import co.cask.tephra.runtime.TransactionClientModule;
import co.cask.tephra.runtime.TransactionModules;
import co.cask.tephra.runtime.ZKModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ThriftTransactionServerTest {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftTransactionSystemTest.class);

  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClientService;
  private static TransactionService txService;
  private static TransactionStateStorage storage;
  private static TransactionSystemClient txClient;
  static Injector injector;

  private static final CountDownLatch WAIT_LATCH = new CountDownLatch(1);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void start() throws Exception {
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    Configuration conf = new Configuration();
    conf.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, false);
    conf.set(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM, zkServer.getConnectionStr());
    conf.set(TxConstants.Service.CFG_DATA_TX_CLIENT_RETRY_STRATEGY, "n-times");
    conf.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_ATTEMPTS, 1);
    conf.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_COUNT, 55);
    conf.setLong(TxConstants.Service.CFG_DATA_TX_CLIENT_TIMEOUT, TimeUnit.HOURS.toMillis(1));

    injector = Guice.createInjector(
      new ConfigModule(conf),
      new ZKModule(),
      new DiscoveryModules().getDistributedModules(),
      Modules.override(new TransactionModules().getDistributedModules())
        .with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(TransactionStateStorage.class).to(SlowTransactionStorage.class).in(Scopes.SINGLETON);
          }
        }),
      new TransactionClientModule()
    );

    zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    // start a tx server
    txService = injector.getInstance(TransactionService.class);
    storage = injector.getInstance(TransactionStateStorage.class);
    txClient = injector.getInstance(TransactionSystemClient.class);
    try {
      LOG.info("Starting transaction service");
      txService.startAndWait();
    } catch (Exception e) {
      LOG.error("Failed to start service: ", e);
    }
  }

  @Before
  public void reset() throws Exception {
    getClient().resetState();
  }

  @AfterClass
  public static void stop() throws Exception {
    txService.stopAndWait();
    storage.stopAndWait();
    zkClientService.stopAndWait();
    zkServer.stopAndWait();
  }

  public TransactionSystemClient getClient() throws Exception {
    return injector.getInstance(TransactionSystemClient.class);
  }

  @Test
  public void testThriftServerStop() throws Exception {
    int nThreads = 55;
    ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
    for (int i = 0; i < nThreads; ++i) {
      final int finalI = i;
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            LOG.error("############## Starting client " + finalI);
            TransactionSystemClient client1 = getClient();
            client1.startShort();
          } catch (Exception e) {
//            e.printStackTrace();
          }
        }
      });
    }
    LOG.error("################# Sleeping for 10 seconds before stopping");
    TimeUnit.SECONDS.sleep(10);
    expireZkSession(zkClientService);
    TimeUnit.SECONDS.sleep(10);
    zkClientService.stopAndWait();
    WAIT_LATCH.countDown();
    TimeUnit.SECONDS.sleep(10);
    LOG.error("2222222222222 ThirftRPCServer is - " + txService.thriftRPCServerState());
  }

  private void expireZkSession(ZKClientService zkClientService) throws Exception {
    LOG.error("222222222222 Expiring zookeeper session");
    ZooKeeper zooKeeper = zkClientService.getZooKeeperSupplier().get();
    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        LOG.info("Processed WatchedEvent: {}", event);
      }
    };
    final ZooKeeper dupZookeeper =
      new ZooKeeper(zkClientService.getConnectString(), zooKeeper.getSessionTimeout(), watcher,
                    zooKeeper.getSessionId(), zooKeeper.getSessionPasswd());
    // wait until connected, then close
    while (dupZookeeper.getState() != ZooKeeper.States.CONNECTED) {
      Thread.sleep(10);
    }
    Assert.assertEquals("Failed to re-create current session", dupZookeeper.getState(), ZooKeeper.States.CONNECTED);
//    dupZookeeper.close();
  }

  private static class SlowTransactionStorage extends InMemoryTransactionStateStorage {
    @Override
    public TransactionLog createLog(long timestamp) throws IOException {
      return new SlowTransactionLog(timestamp);
    }
  }

  private static class SlowTransactionLog extends InMemoryTransactionStateStorage.InMemoryTransactionLog {
    public SlowTransactionLog(long timestamp) {
      super(timestamp);
    }

    @Override
    public void append(TransactionEdit edit) throws IOException {
      try {
        LOG.error("1111111111111111 Waiting in wait latch");
        WAIT_LATCH.await();
        LOG.error("1111111111111111 Done waiting in wait latch");
      } catch (InterruptedException e) {
        LOG.error("Got exception: ", e);
      }
      super.append(edit);
    }

    @Override
    public void append(List<TransactionEdit> edits) throws IOException {
      try {
        LOG.error("1111111111111111 Waiting in wait latch");
        WAIT_LATCH.await();
        LOG.error("1111111111111111 Done waiting in wait latch");
      } catch (InterruptedException e) {
        LOG.error("Got exception: ", e);
      }
      super.append(edits);
    }
  }
}
