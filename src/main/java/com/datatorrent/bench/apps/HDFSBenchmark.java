/**
 * Put your copyright and license info here.
 */
package com.datatorrent.bench.apps;

import com.datatorrent.bench.FsWriteOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.testbench.SeedEventGenerator;

@ApplicationAnnotation(name="HDFSBenchmark")
public class HDFSBenchmark implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.addOperator("BenchMark", new FsWriteOperator());
  }
}
