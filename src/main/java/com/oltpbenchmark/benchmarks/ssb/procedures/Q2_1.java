package com.oltpbenchmark.benchmarks.ssb.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q2_1 extends GenericQuery {
  public final SQLStmt query_stmt =
    new SQLStmt(
      """
           select sum(lo_revenue), d_year, p_brand1
           from lineorder, date, part, supplier
              where lo_orderdate = d_datekey
              and lo_partkey = p_partkey
              and lo_suppkey = s_suppkey
              and p_category = 'MFGR#12'
              and s_region = 'AMERICA'
           group by d_year, p_brand1
           order by d_year, p_brand1;
        """);

  @Override
  protected PreparedStatement getStatement(
    Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
    return this.getPreparedStatement(conn, query_stmt);
  }
}
