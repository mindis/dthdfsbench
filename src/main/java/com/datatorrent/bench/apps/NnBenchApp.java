
package com.datatorrent.bench.apps;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.bench.NnBenchOperator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="NnBenchApp")
public class NnBenchApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.addOperator("BenchMark", new NnBenchOperator());
  }
}
