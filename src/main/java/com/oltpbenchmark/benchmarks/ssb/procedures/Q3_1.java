package com.oltpbenchmark.benchmarks.ssb.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q3_1 extends GenericQuery {
  public final SQLStmt query_stmt =
    new SQLStmt(
      """
          select c_nation, s_nation, d_year, sum(lo_revenue) as revenue
          from customer, lineorder, supplier, date
            where lo_custkey = c_custkey
              and lo_suppkey = s_suppkey
              and lo_orderdate = d_datekey
              and c_region = 'ASIA' and s_region = 'ASIA'
              and d_year >= 1992 and d_year <= 1997
          group by c_nation, s_nation, d_year
          order by d_year asc, revenue desc;
        """);

  @Override
  protected PreparedStatement getStatement(
    Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
    return this.getPreparedStatement(conn, query_stmt);
  }
}
