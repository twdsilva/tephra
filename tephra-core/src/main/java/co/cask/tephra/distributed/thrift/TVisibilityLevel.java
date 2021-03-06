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

/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package co.cask.tephra.distributed.thrift;


public enum TVisibilityLevel implements org.apache.thrift.TEnum {
  SNAPSHOT(1),
  SNAPSHOT_EXCLUDE_CURRENT(2),
  SNAPSHOT_ALL(3);

  private final int value;

  private TVisibilityLevel(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static TVisibilityLevel findByValue(int value) { 
    switch (value) {
      case 1:
        return SNAPSHOT;
      case 2:
        return SNAPSHOT_EXCLUDE_CURRENT;
      case 3:
        return SNAPSHOT_ALL;
      default:
        return null;
    }
  }
}
