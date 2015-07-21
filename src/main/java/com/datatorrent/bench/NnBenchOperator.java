package com.datatorrent.bench;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Run metadata operation, and print stats at the end.
 */
public class NnBenchOperator extends BaseOperator implements InputOperator, Operator.CheckpointListener
{
  int numFiles = 1000;
  private transient FileSystem fs;
  public String pathStr = "fsbench";
  private transient Path path;
  private int operatorId;
  private String type = "write,rename,delete";

  private transient List<Path> paths = Lists.newArrayList();

  @Override public void emitTuples()
  {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override public void setup(Context.OperatorContext context)
  {
    try {
      operatorId = context.getId();
      path = new Path(pathStr);
      fs = FileSystem.newInstance(path.toUri(), new Configuration());
      path = new Path(path, String.valueOf(operatorId));
      super.setup(context);
      Thread thrd = new Thread() {
        public void run() {
          try {
            runBenchmark();
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        }
      };
      thrd.run();
    } catch (Exception ex) {
      throw new RuntimeException("Unable to initialize filesystem");
    }
  }

  void runBenchmark() throws IOException
  {
    if (type.contains("write")) {
      writeFiles();
    }
    if (type.contains("rename")) {
      renameFiles();
    }
    if (type.contains("delete")) {
      deleteFiles();
    }
  }

  private void deleteFiles() throws IOException
  {
    long start = System.currentTimeMillis();
    for(Path p : paths) {
      fs.delete(p, false);
    }
    long end = System.currentTimeMillis();
    logger.info("time take to delete {} files is {}", paths.size(), (end - start));
  }

  private void renameFiles() throws IOException
  {
    List<Path> renamed = Lists.newArrayList();
    long start = System.currentTimeMillis();
    for(Path p : paths) {
      Path ren = new Path(p + ".renamed");
      fs.rename(p, ren);
      renamed.add(ren);
    }
    long end = System.currentTimeMillis();
    paths = renamed;
    logger.info("time taken for rename {} files {}", paths.size(), (end-start));
  }

  private void writeFiles() throws IOException
  {
    long start = System.currentTimeMillis();
    for(int i = 0; i < numFiles; i++) {
      Path curr = new Path(path, operatorId + "/file" + i);
      fs.create(curr);
      paths.add(curr);
    }
    long end = System.currentTimeMillis();
    logger.info("time take to create {} files is {}", numFiles, (end - start));
  }

  @Override public void checkpointed(long windowId)
  {

  }

  @Override public void committed(long windowId)
  {

  }

  private static final Logger logger = LoggerFactory.getLogger(FsWriteOperator.class);

}
