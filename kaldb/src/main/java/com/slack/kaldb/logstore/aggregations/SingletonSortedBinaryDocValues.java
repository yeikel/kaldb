/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package com.slack.kaldb.logstore.aggregations;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Provider for sorted binary docvalues
 *
 * @opensearch.internal
 */
final class SingletonSortedBinaryDocValues extends SortedBinaryDocValues {

    private final BinaryDocValues in;

    SingletonSortedBinaryDocValues(BinaryDocValues in) {
        this.in = in;
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        return in.advanceExact(doc);
    }

    @Override
    public int docValueCount() {
        return 1;
    }

    @Override
    public BytesRef nextValue() throws IOException {
        return in.binaryValue();
    }

    public BinaryDocValues getBinaryDocValues() {
        return in;
    }

}
