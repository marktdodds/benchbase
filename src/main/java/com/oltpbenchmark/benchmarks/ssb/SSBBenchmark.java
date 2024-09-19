package com.oltpbenchmark.benchmarks.ssb;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.ssb.procedures.Q1_1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SSBBenchmark extends BenchmarkModule {
  private static final Logger LOG = LoggerFactory.getLogger(SSBBenchmark.class);

  public SSBBenchmark(WorkloadConfiguration workConf) {
    super(workConf);
  }

  @Override
  protected Package getProcedurePackageImpl() {
    return (Q1_1.class.getPackage());
  }

  @Override
  protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
    List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();

    int numTerminals = workConf.getTerminals();
    LOG.info(String.format("Creating %d workers for TPC-H", numTerminals));
    for (int i = 0; i < numTerminals; i++) {
      workers.add(new SSBWorker(this, i));
    }
    return workers;
  }

  @Override
  protected Loader<SSBBenchmark> makeLoaderImpl() {
    return new SSBLoader(this);
  }
}
