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

/**
 *   SSB implementation
 *   Mark Dodds (marktdodds@gmail.com)
 *
 **/

package com.oltpbenchmark.benchmarks.ssb;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.oltpbenchmark.benchmarks.ssb.SSBConstants.TABLENAME_CUSTOMER;
import static com.oltpbenchmark.benchmarks.ssb.SSBConstants.TABLENAME_DATE;
import static com.oltpbenchmark.benchmarks.ssb.SSBConstants.TABLENAME_LINEORDER;
import static com.oltpbenchmark.benchmarks.ssb.SSBConstants.TABLENAME_PART;
import static com.oltpbenchmark.benchmarks.ssb.SSBConstants.TABLENAME_SUPPLIER;


public final class SSBLoader extends Loader<SSBBenchmark> {
  private enum CastTypes {
    INTEGER,
    DOUBLE,
    STRING,
  }

  private static final CastTypes[] LINEORDER_TYPES = {
    CastTypes.INTEGER, // LO_ORDERKEY
    CastTypes.INTEGER, // LO_LINENUMBER
    CastTypes.INTEGER, // LO_CUSTKEY
    CastTypes.INTEGER, // LO_PARTKEY
    CastTypes.INTEGER, // LO_SUPPKEY
    CastTypes.INTEGER, // LO_ORDERDATE
    CastTypes.STRING, // LO_ORDERPRIORITY
    CastTypes.STRING, // LO_SHIPPRIORITY
    CastTypes.INTEGER, // LO_QUANTITY
    CastTypes.INTEGER, // LO_EXTENDEDPRICE
    CastTypes.INTEGER, // LO_ORDTOTALPRICE
    CastTypes.INTEGER, // LO_DISCOUNT
    CastTypes.INTEGER, // LO_REVENUE
    CastTypes.INTEGER, // LO_SUPPLYCOST
    CastTypes.INTEGER, // LO_TAX
    CastTypes.INTEGER, // LO_COMMITDATE
    CastTypes.STRING // LO_SHIPMODE
  };

  private static final CastTypes[] DATE_TYPES = {
    CastTypes.INTEGER,  // D_DATEKEY
    CastTypes.STRING,  // D_DATE
    CastTypes.STRING,  // D_DAYOFWEEK
    CastTypes.STRING,  // D_MONTH
    CastTypes.INTEGER,  // D_YEAR
    CastTypes.INTEGER,  // D_YEARMONTHNUM
    CastTypes.STRING,   // D_YEARMONTH
    CastTypes.INTEGER,   // D_DAYNUMINWEEK
    CastTypes.INTEGER,  // D_DAYNUMINMONTH
    CastTypes.INTEGER,  // D_DAYNUMINYEAR
    CastTypes.INTEGER,  // D_MONTHNUMINYEAR
    CastTypes.INTEGER,  // D_WEEKNUMINYEAR
    CastTypes.STRING,  // D_SELLINGSEASON
    CastTypes.INTEGER,  // D_LASTDAYINWEEKFL
    CastTypes.INTEGER,  // D_LASTDAYINMONTHFL
    CastTypes.INTEGER,  // D_HOLIDAYFL
    CastTypes.INTEGER    // D_WEEKDAYFL
  };

  private static final CastTypes[] PART_TYPES = {
    CastTypes.INTEGER, // P_PARTKEY
    CastTypes.STRING, // P_NAME
    CastTypes.STRING, // P_MFGR
    CastTypes.STRING, // P_CATEGORY
    CastTypes.STRING, // P_BRAND
    CastTypes.STRING, // P_COLOR
    CastTypes.STRING, // P_TYPE
    CastTypes.STRING, // P_SIZE
    CastTypes.STRING, // P_CONTAINER
  };

  private static final CastTypes[] SUPPLIER_TYPES = {
    CastTypes.INTEGER, // S_SUPPKEY
    CastTypes.STRING, // S_NAME
    CastTypes.STRING, // S_ADDRESS
    CastTypes.STRING, // S_CITY
    CastTypes.STRING, // S_NATION
    CastTypes.STRING, // S_REGION
    CastTypes.STRING, // S_PHONE
  };

  private static final CastTypes[] CUSTOMER_TYPES = {
    CastTypes.INTEGER, // C_CUSTKEY
    CastTypes.STRING, // C_NAME
    CastTypes.STRING, // C_ADDRESS
    CastTypes.STRING, // C_CITY
    CastTypes.STRING, // C_NATION
    CastTypes.STRING, // C_REGION
    CastTypes.STRING, // C_PHONE
    CastTypes.STRING, // C_MKTSEGMENT
  };

  public SSBLoader(SSBBenchmark benchmark) {
    super(benchmark);
  }

  private PreparedStatement getInsertStatement(Connection conn, String tableName)
    throws SQLException {
    Table catalog_tbl = benchmark.getCatalog().getTable(tableName);
    String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
    return conn.prepareStatement(sql);
  }

  @Override
  public List<LoaderThread> createLoaderThreads() {

    ArrayList<LoaderThread> threads = new ArrayList<>();

    // DATE
    threads.add(
      new LoaderThread(this.benchmark) {
        @Override
        public void load(Connection conn) throws SQLException {
          try (PreparedStatement statement = getInsertStatement(conn, TABLENAME_DATE)) {
            List<Iterable<List<String>>> lineItemGenerators = new ArrayList<>();
            try {
              lineItemGenerators.add(new FileIterable(new File("data/ssb-sf" + benchmark.getWorkloadConfiguration().getScaleFactor() + "/date.tbl")));
              genTable(statement, lineItemGenerators, DATE_TYPES, TABLENAME_DATE);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      });

    // PART
    threads.add(
      new LoaderThread(this.benchmark) {
        @Override
        public void load(Connection conn) throws SQLException {
          try (PreparedStatement statement = getInsertStatement(conn, TABLENAME_PART)) {
            List<Iterable<List<String>>> lineItemGenerators = new ArrayList<>();
            try {
              lineItemGenerators.add(new FileIterable(new File("data/ssb-sf" + benchmark.getWorkloadConfiguration().getScaleFactor() + "/part.tbl")));
              genTable(statement, lineItemGenerators, PART_TYPES, TABLENAME_PART);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      });

    // SUPPLIER
    threads.add(
      new LoaderThread(this.benchmark) {
        @Override
        public void load(Connection conn) throws SQLException {
          try (PreparedStatement statement = getInsertStatement(conn, TABLENAME_SUPPLIER)) {
            List<Iterable<List<String>>> lineItemGenerators = new ArrayList<>();
            try {
              lineItemGenerators.add(new FileIterable(new File("data/ssb-sf" + benchmark.getWorkloadConfiguration().getScaleFactor() + "/supplier.tbl")));
              genTable(statement, lineItemGenerators, SUPPLIER_TYPES, TABLENAME_SUPPLIER);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      });

    // CUSTOMER
    threads.add(
      new LoaderThread(this.benchmark) {
        @Override
        public void load(Connection conn) throws SQLException {
          try (PreparedStatement statement = getInsertStatement(conn, TABLENAME_CUSTOMER)) {
            List<Iterable<List<String>>> lineItemGenerators = new ArrayList<>();
            try {
              lineItemGenerators.add(new FileIterable(new File("data/ssb-sf" + benchmark.getWorkloadConfiguration().getScaleFactor() + "/customer.tbl")));
              genTable(statement, lineItemGenerators, CUSTOMER_TYPES, TABLENAME_CUSTOMER);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      });

    // LINEORDER
    threads.add(
      new LoaderThread(this.benchmark) {
        @Override
        public void load(Connection conn) throws SQLException {
          try (PreparedStatement statement = getInsertStatement(conn, TABLENAME_LINEORDER)) {
            List<Iterable<List<String>>> lineItemGenerators = new ArrayList<>();
            try {
              lineItemGenerators.add(new FileIterable(new File("data/ssb-sf" + benchmark.getWorkloadConfiguration().getScaleFactor() + "/lineorder.tbl")));
              genTable(statement, lineItemGenerators, LINEORDER_TYPES, TABLENAME_LINEORDER);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      });


    return threads;
  }


  private void genTable(
    PreparedStatement prepStmt,
    List<Iterable<List<String>>> generators,
    CastTypes[] types,
    String tableName) {
    for (Iterable<List<String>> generator : generators) {
      try {
        int recordsRead = 0;
        for (List<String> elems : generator) {
          for (int idx = 0; idx < types.length; idx++) {
            final CastTypes type = types[idx];
            switch (type) {
              case DOUBLE:
                prepStmt.setDouble(idx + 1, Double.parseDouble(elems.get(idx)));
                break;
              case INTEGER:
                prepStmt.setInt(idx + 1, Integer.parseInt(elems.get(idx)));
                break;
              case STRING:
                prepStmt.setString(idx + 1, elems.get(idx));
                break;
              default:
                throw new RuntimeException("Unrecognized type for prepared statement");
            }
          }

          ++recordsRead;
          prepStmt.addBatch();
          if ((recordsRead % workConf.getBatchSize()) == 0) {

            LOG.debug("writing batch {} for table {}", recordsRead, tableName);

            prepStmt.executeBatch();
            prepStmt.clearBatch();
          }
        }

        prepStmt.executeBatch();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }


  static class FileIterable implements Iterable<List<String>> {

    private final BufferedReader reader;
    private String nextLine;

    public FileIterable(File file) throws FileNotFoundException {
      reader = new BufferedReader(new FileReader(file));
      advance();
    }

    @Override
    public @NotNull Iterator<List<String>> iterator() {
      return new Iterator<>() {
        @Override
        public boolean hasNext() {
          return (nextLine != null);
        }

        @Override
        public List<String> next() {
          String line = nextLine;
          advance();
          return Arrays.asList(line.split("\\|"));
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    private void advance() {
      try {
        nextLine = reader.readLine();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
