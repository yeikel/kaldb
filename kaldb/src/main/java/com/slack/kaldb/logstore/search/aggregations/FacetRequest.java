/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.slack.kaldb.logstore.search.aggregations;

public abstract class FacetRequest {

  public static enum SortDirection {
    asc(-1),
    desc(1);

    private final int multiplier;

    private SortDirection(int multiplier) {
      this.multiplier = multiplier;
    }

    public static SortDirection fromObj(Object direction) {
      if (direction == null) {
        // should we just default either to desc/asc??
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing Sort direction");
      }

      switch (direction.toString()) {
        case "asc":
          return asc;
        case "desc":
          return desc;
        default:
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST, "Unknown Sort direction '" + direction + "'");
      }
    }

    // asc==-1, desc==1
    public int getMultiplier() {
      return multiplier;
    }
  }
}
