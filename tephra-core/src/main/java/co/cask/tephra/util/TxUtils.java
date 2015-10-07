/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.tephra.util;

import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionType;
import co.cask.tephra.TxConstants;
import co.cask.tephra.persist.TransactionSnapshot;
import com.google.common.primitives.Longs;

import java.util.Map;

/**
 * Utility methods supporting transaction operations.
 */
public class TxUtils {

  // Any cell with timestamp less than MAX_NON_TX_TIMESTAMP is assumed to be pre-existing data,
  // i.e. data written before table was converted into transactional table using Tephra.
  // Using 1.1 times current time to determine whether a timestamp is transactional timestamp or not is safe, and does
  // not produce any false positives or false negatives.
  //
  // To prove this, let's say the earliest transactional timestamp written by Tephra was in year 2000, and the oldest
  // that will be written is in year 2200.
  // 01-Jan-2000 GMT is  946684800000 milliseconds since epoch.
  // 31-Dec-2200 GMT is 7289654399000 milliseconds since epoch.
  //
  // Let's say that we enabled transactions on a table on 01-Jan-2000, then 1.1 * 946684800000 = 31-Dec-2002. Using
  // 31-Dec-2002, we can safely say from 01-Jan-2000 onwards, whether a cell timestamp is
  // non-transactional (<= 946684800000).
  // Note that transactional timestamp will be greater than 946684800000000000 (> year 31969) at this instant.
  //
  // On the other end, let's say we enabled transactions on a table on 31-Dec-2200,
  // then 1.1 * 7289654399000 = 07-Feb-2224. Again, we can use this time from 31-Dec-2200 onwards to say whether a
  // cell timestamp is transactional (<= 7289654399000).
  // Note that transactional timestamp will be greater than 7289654399000000000 (> year 232969) at this instant.
  private static final long MAX_NON_TX_TIMESTAMP = (long) (System.currentTimeMillis() * 1.1);

  /**
   * Returns the oldest visible timestamp for the given transaction, based on the TTLs configured for each column
   * family.  If no TTL is set on any column family, the oldest visible timestamp will be {@code 0}.
   * @param ttlByFamily A map of column family name to TTL value (in milliseconds)
   * @param tx The current transaction
   * @return The oldest timestamp that will be visible for the given transaction and TTL configuration
   */
  public static long getOldestVisibleTimestamp(Map<byte[], Long> ttlByFamily, Transaction tx) {
    long oldestVisible = tx.getVisibilityUpperBound();
    // we know that data will not be cleaned up while this tx is running up to this point as janitor uses it
    for (Long familyTTL : ttlByFamily.values()) {
      oldestVisible =
        Math.min(familyTTL <= 0 ? 0 : tx.getVisibilityUpperBound() - familyTTL * TxConstants.MAX_TX_PER_MS,
                 oldestVisible);
    }
    return oldestVisible;
  }

  /**
   * Returns the maximum timestamp to use for time-range operations, based on the given transaction.
   * @param tx The current transaction
   * @return The maximum timestamp (exclusive) to use for time-range operations
   */
  public static long getMaxVisibleTimestamp(Transaction tx) {
    // NOTE: +1 here because we want read up to writepointer inclusive, but timerange's end is exclusive
    // however, we also need to guard against overflow in the case write pointer is set to MAX_VALUE
    return tx.getWritePointer() < Long.MAX_VALUE ?
        tx.getWritePointer() + 1 : tx.getWritePointer();
  }

  /**
   * Creates a "dummy" transaction based on the given snapshot's state.  This is not a "real" transaction in the
   * sense that it has not been started, data should not be written with it, and it cannot be committed.  However,
   * this can still be useful for filtering data according to the snapshot's state.  Instead of the actual
   * write pointer from the snapshot, however, we use {@code Long.MAX_VALUE} to avoid mis-identifying any cells as
   * being written by this transaction (and therefore visible).
   */
  public static Transaction createDummyTransaction(TransactionSnapshot snapshot) {
    return new Transaction(snapshot.getReadPointer(), Long.MAX_VALUE,
                           Longs.toArray(snapshot.getInvalid()),
                           Longs.toArray(snapshot.getInProgress().keySet()),
                           TxUtils.getFirstShortInProgress(snapshot.getInProgress()), TransactionType.SHORT);
  }

  /**
   * Returns the write pointer for the first "short" transaction that in the in-progress set, or
   * {@link Transaction#NO_TX_IN_PROGRESS} if none.
   */
  public static long getFirstShortInProgress(Map<Long, TransactionManager.InProgressTx> inProgress) {
    long firstShort = Transaction.NO_TX_IN_PROGRESS;
    for (Map.Entry<Long, TransactionManager.InProgressTx> entry : inProgress.entrySet()) {
      if (!entry.getValue().isLongRunning()) {
        firstShort = entry.getKey();
        break;
      }
    }
    return firstShort;
  }

  /**
   * Returns the timestamp for calculating time to live for the given cell timestamp.
   * This takes into account pre-existing non-transactional cells while calculating the time.
   */
  public static long getTimestampForTTL(long cellTs) {
    return cellTs < MAX_NON_TX_TIMESTAMP ? cellTs * TxConstants.MAX_TX_PER_MS : cellTs;
  }
}
