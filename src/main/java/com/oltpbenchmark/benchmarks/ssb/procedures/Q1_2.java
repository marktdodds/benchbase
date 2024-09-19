package com.oltpbenchmark.benchmarks.ssb.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q1_2 extends GenericQuery {
  public final SQLStmt query_stmt =
    new SQLStmt(
      """
          select sum(lo_extendedprice*lo_discount) as revenue
          from lineorder, date
            where lo_orderdate = d_datekey
            and d_yearmonthnum = 199401
            and lo_discount between 4 and 6
            and lo_quantity between 26 and 35;
        """);

  @Override
  protected PreparedStatement getStatement(
    Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
    return this.getPreparedStatement(conn, query_stmt);
  }
}
