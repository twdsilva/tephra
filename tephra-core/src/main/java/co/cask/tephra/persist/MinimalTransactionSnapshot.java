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

package co.cask.tephra.persist;

import co.cask.tephra.TransactionManager;
import com.google.common.base.Objects;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Represents an in-memory snapshot of the transaction state required for Coprocessors to perform cleanup.
 */
public class MinimalTransactionSnapshot {
  private long timestamp;
  private long readPointer;
  private long writePointer;
  private Collection<Long> invalid;
  private NavigableMap<Long, TransactionManager.InProgressTx> inProgress;


  public MinimalTransactionSnapshot(long timestamp, long readPointer, long writePointer, Collection<Long> invalid,
                                    NavigableMap<Long, TransactionManager.InProgressTx> inProgress) {
    this.timestamp = timestamp;
    this.readPointer = readPointer;
    this.writePointer = writePointer;
    this.invalid = invalid;
    this.inProgress = inProgress;
  }

  /**
   * Returns the timestamp from when this snapshot was created.
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Returns the read pointer at the time of the snapshot.
   */
  public long getReadPointer() {
    return readPointer;
  }

  /**
   * Returns the next write pointer at the time of the snapshot.
   */
  public long getWritePointer() {
    return writePointer;
  }

  /**
   * Returns the list of invalid write pointers at the time of the snapshot.
   */
  public Collection<Long> getInvalid() {
    return invalid;
  }

  /**
   * Returns the map of in-progress transaction write pointers at the time of the snapshot.
   * @return a map of write pointer to expiration timestamp (in milliseconds) for all transactions in-progress.
   */
  public NavigableMap<Long, TransactionManager.InProgressTx> getInProgress() {
    return inProgress;
  }

  /**
   * @return transaction id {@code X} such that any of the transactions newer than {@code X} might be invisible to
   *         some of the currently in-progress transactions or to those that will be started <p>
   *         NOTE: the returned tx id can be invalid.
   */
  public long getVisibilityUpperBound() {
    // the readPointer of the oldest in-progress tx is the oldest in use
    // todo: potential problem with not moving visibility upper bound for the whole duration of long-running tx
    Map.Entry<Long, TransactionManager.InProgressTx> firstInProgress = inProgress.firstEntry();
    if (firstInProgress == null) {
      // using readPointer as smallest visible when non txs are there
      return readPointer;
    }
    return firstInProgress.getValue().getVisibilityUpperBound();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("timestamp", timestamp)
      .add("readPointer", readPointer)
      .add("writePointer", writePointer)
      .add("invalidSize", invalid.size())
      .add("inProgressSize", inProgress.size())
      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(readPointer, writePointer, invalid, inProgress);
  }

  /**
   * Checks that this instance matches another {@link MinimalTransactionSnapshot} instance.
   * Note that the equality check ignores the snapshot timestamp value, but includes all other properties.
   *
   * @param obj the other instance to check for equality.
   * @return {@code true} if the instances are equal, {@code false} if not.
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof  MinimalTransactionSnapshot)) {
      return false;
    }
    MinimalTransactionSnapshot other = (MinimalTransactionSnapshot) obj;
    return getReadPointer() == other.getReadPointer() &&
      getWritePointer() == other.getWritePointer() &&
      getInvalid().equals(other.getInvalid()) &&
      getInProgress().equals(other.getInProgress());
  }
}
