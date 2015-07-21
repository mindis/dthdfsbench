/**
 * Put your copyright and license info here.
 */
package com.datatorrent.bench;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import com.datatorrent.bench.apps.HDFSBenchmark;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.LocalMode;

/**
 * Test the DAG declaration in local mode.
 */
public class HDFSBenchmarkTest
{

  @Test
  public void testApplication() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new HDFSBenchmark(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
