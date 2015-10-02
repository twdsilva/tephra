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

package co.cask.tephra.snapshot;

import co.cask.tephra.persist.MinimalTransactionSnapshot;
import co.cask.tephra.persist.TransactionSnapshot;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface to decode and encode a transaction snapshot. Each codec implements one version of the encoding.
 * It need not include the version when encoding the snapshot.
 */
public interface SnapshotCodec {

  /**
   * @return the version of the encoding implemented by the codec.
   */
  int getVersion();

  /**
   * Encode a transaction snapshot into an output stream.
   * @param out the output stream to write to
   * @param snapshot the snapshot to encode
   */
  void encode(OutputStream out, TransactionSnapshot snapshot);

  /**
   * Decode a transaction snapshot from an input stream.
   * @param in the input stream to read from
   * @return the decoded snapshot
   */
  TransactionSnapshot decode(InputStream in);

  /**
   * Decode a minimal transaction snapshot from an input stream.
   * @param in the input stream to read from
   * @return {@link MinimalTransactionSnapshot}
   */
  MinimalTransactionSnapshot decodeMinimalSnapshot(InputStream in);
}
