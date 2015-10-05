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

package co.cask.tephra.persist;

import co.cask.tephra.ChangeId;
import co.cask.tephra.TransactionManager;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Represents an in-memory snapshot of the full transaction state.
 */
public class TransactionSnapshot extends MinimalTransactionSnapshot {

  private Map<Long, Set<ChangeId>> committingChangeSets;
  private Map<Long, Set<ChangeId>> committedChangeSets;

  public TransactionSnapshot(long timestamp, long readPointer, long writePointer, Collection<Long> invalid,
                             NavigableMap<Long, TransactionManager.InProgressTx> inProgress,
                             Map<Long, Set<ChangeId>> committing, Map<Long, Set<ChangeId>> committed) {
    super(timestamp, readPointer, writePointer, invalid, inProgress);
    this.committingChangeSets = committing;
    this.committedChangeSets = committed;
  }

  /**
   * Returns a map of transaction write pointer to sets of changed row keys for transactions that had called
   * {@code InMemoryTransactionManager.canCommit(Transaction, Collection)} but not yet called
   * {@code InMemoryTransactionManager.commit(Transaction)} at the time of the snapshot.
   *
   * @return a map of transaction write pointer to set of changed row keys.
   */
  public Map<Long, Set<ChangeId>> getCommittingChangeSets() {
    return committingChangeSets;
  }

  /**
   * Returns a map of transaction write pointer to set of changed row keys for transaction that had successfully called
   * {@code InMemoryTransactionManager.commit(Transaction)} at the time of the snapshot.
   *
   * @return a map of transaction write pointer to set of changed row keys.
   */
  public Map<Long, Set<ChangeId>> getCommittedChangeSets() {
    return committedChangeSets;
  }

  /**
   * Checks that this instance matches another {@code TransactionSnapshot} instance.  Note that the equality check
   * ignores the snapshot timestamp value, but includes all other properties.
   *
   * @param obj the other instance to check for equality.
   * @return {@code true} if the instances are equal, {@code false} if not.
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof  TransactionSnapshot)) {
      return false;
    }
    TransactionSnapshot other = (TransactionSnapshot) obj;
    return super.equals(obj) &&
      committingChangeSets.equals(other.committingChangeSets) &&
      committedChangeSets.equals(other.committedChangeSets);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("timestamp", getTimestamp())
        .add("readPointer", getReadPointer())
        .add("writePointer", getWritePointer())
        .add("invalidSize", getInvalid().size())
        .add("inProgressSize", getInProgress().size())
        .add("committingSize", committingChangeSets.size())
        .add("committedSize", committedChangeSets.size())
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getReadPointer(), getWritePointer(), getInvalid(), getInProgress(),
                            committingChangeSets, committedChangeSets);
  }

  /**
   * Creates a new {@code TransactionSnapshot} instance with copies of all of the individual collections.
   * @param readPointer current transaction read pointer
   * @param writePointer current transaction write pointer
   * @param invalid current list of invalid write pointers
   * @param inProgress current map of in-progress write pointers to expiration timestamps
   * @param committing current map of write pointers to change sets which have passed {@code canCommit()} but not
   *                   yet committed
   * @param committed current map of write pointers to change sets which have committed
   * @return a new {@code TransactionSnapshot} instance
   */
  public static TransactionSnapshot copyFrom(long snapshotTime, long readPointer,
                                             long writePointer, Collection<Long> invalid,
                                             NavigableMap<Long, TransactionManager.InProgressTx> inProgress,
                                             Map<Long, Set<ChangeId>> committing,
                                             NavigableMap<Long, Set<ChangeId>> committed) {
    // copy invalid IDs
    Collection<Long> invalidCopy = Lists.newArrayList(invalid);
    // copy in-progress IDs and expirations
    NavigableMap<Long, TransactionManager.InProgressTx> inProgressCopy = Maps.newTreeMap(inProgress);

    // for committing and committed maps, we need to copy each individual Set as well to prevent modification
    Map<Long, Set<ChangeId>> committingCopy = Maps.newHashMap();
    for (Map.Entry<Long, Set<ChangeId>> entry : committing.entrySet()) {
      committingCopy.put(entry.getKey(), new HashSet<>(entry.getValue()));
    }

    NavigableMap<Long, Set<ChangeId>> committedCopy = new TreeMap<>();
    for (Map.Entry<Long, Set<ChangeId>> entry : committed.entrySet()) {
      committedCopy.put(entry.getKey(), new HashSet<>(entry.getValue()));
    }

    return new TransactionSnapshot(snapshotTime, readPointer, writePointer,
                                   invalidCopy, inProgressCopy, committingCopy, committedCopy);
  }
}
