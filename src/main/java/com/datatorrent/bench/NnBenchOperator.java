package com.datatorrent.bench;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Run metadata operation, and print stats at the end.
 */
public class NnBenchOperator extends BaseOperator implements InputOperator, Operator.CheckpointListener
{
  int numFiles = 100;
  private transient FileSystem fs;
  public String pathStr = "fsbench";
  private transient Path path;
  private int operatorId;
  private String type = "write,rename,delete";

  private transient List<Path> paths = Lists.newArrayList();
  private int fileSize = 0;
  private byte[] buffer;

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
      fs.delete(path, true);
      buffer = getRandomBuffer();
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
      thrd.start();
    } catch (Exception ex) {
      throw new RuntimeException("Unable to initialize filesystem");
    }
  }


  private byte[] getRandomBuffer()
  {
    byte[] bytes = new byte[fileSize];
    Random r = new Random();
    r.nextBytes(bytes);
    return bytes;
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
      logger.info("deleting file {}", p);
      fs.delete(p, false);
    }
    long end = System.currentTimeMillis();
    long diff = end - start;
    logger.info("time take to delete {} files is {} avg {}", paths.size(), diff, (double)diff / paths.size());
  }

  private void renameFiles() throws IOException
  {
    List<Path> renamed = Lists.newArrayList();
    long start = System.currentTimeMillis();
    for(Path p : paths) {
      Path ren = new Path(p + ".renamed");
      logger.info("renaming file {} to {}", p, ren);
      fs.rename(p, ren);
      renamed.add(ren);
    }
    long end = System.currentTimeMillis();
    paths = renamed;
    long diff = end - start;
    logger.info("time taken for rename {} files {} avg {}", paths.size(), diff, (double)diff/paths.size());
  }

  private void writeFiles() throws IOException
  {
    long start = System.currentTimeMillis();
    for(int i = 0; i < numFiles; i++) {
      Path curr = new Path(path, operatorId + "/file" + i);
      FSDataOutputStream out = null;
      try {
        out = fs.create(curr);
        if (fileSize > 0) {
          out.write(buffer);
        }
      } finally {
        if (out != null)
          out.close();
      }
      paths.add(curr);
    }
    long end = System.currentTimeMillis();
    long diff = end - start;
    logger.info("time take to create {} files is {} avg {}", numFiles, diff, (double)diff/paths.size());
  }

  @Override public void checkpointed(long windowId)
  {

  }

  @Override public void committed(long windowId)
  {

  }

  private static final Logger logger = LoggerFactory.getLogger(NnBenchOperator.class);

  public int getNumFiles()
  {
    return numFiles;
  }

  public void setNumFiles(int numFiles)
  {
    this.numFiles = numFiles;
  }

  public String getPathStr()
  {
    return pathStr;
  }

  public void setPathStr(String pathStr)
  {
    this.pathStr = pathStr;
  }

  public String getType()
  {
    return type;
  }

  public void setType(String type)
  {
    this.type = type;
  }

  public int getFileSize()
  {
    return fileSize;
  }

  public void setFileSize(int fileSize)
  {
    this.fileSize = fileSize;
  }
}
