/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/***
 *   TPC-H implementation
 *
 *   Ben Reilly (bd.reilly@gmail.com)
 *   Ippokratis Pandis (ipandis@us.ibm.com)
 *
 ***/

package com.oltpbenchmark.benchmarks.ssb;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class SSBLoader extends Loader<SSBBenchmark> {
  public SSBLoader(SSBBenchmark benchmark) {
    super(benchmark);
  }


  private PreparedStatement getInsertStatement(Connection conn, String tableName)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<LoaderThread> createLoaderThreads() {
    return new ArrayList<>();
  }

}
