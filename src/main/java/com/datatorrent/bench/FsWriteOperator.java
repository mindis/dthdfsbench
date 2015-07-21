package com.datatorrent.bench;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class FsWriteOperator extends BaseOperator implements InputOperator, Operator.CheckpointListener
{
  private transient FileSystem fs;
  public String pathStr = "fsbench";
  public int blockSize = 1024 * 1024;
  private transient Path path;

  private transient byte[] buffer;
  private transient FSDataOutputStream out;
  private int totalWritten = 0;
  private transient long start = 0;
  private int maxFileSize = 100 * 1024 * 1024;
  private int currentFileSize = 0;
  private int fileId = 0;
  private int operatorId = 0;
  private boolean flushEnabled = false;
  private boolean renameAtEnd = true;
  private transient Path currentPath;
  private long renameTime = 0;
  private int totalRenames = 0;

  @Override public void setup(Context.OperatorContext context)
  {
    try {
      operatorId = context.getId();
      path = new Path(pathStr);
      fs = FileSystem.newInstance(path.toUri(), new Configuration());
      buffer = getRandomBuffer();
      out = null;
      start = System.currentTimeMillis();
      super.setup(context);
    } catch (Exception ex) {
      throw new RuntimeException("Unable to initialize filesystem");
    }
  }

  private byte[] getRandomBuffer()
  {
    byte[] bytes = new byte[blockSize];
    Random r = new Random();
    r.nextBytes(bytes);
    return bytes;
  }

  @Override public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  @Override public void emitTuples()
  {
    try {
      if (out == null) {
        currentPath = new Path(path, String.valueOf(operatorId) + "/" + String.valueOf(fileId));
        logger.info("creating new file {}", currentPath);
        out = fs.create(currentPath);
        currentFileSize = 0;
        fileId++;
      }
      out.write(buffer);
      totalWritten += buffer.length;
      currentFileSize += buffer.length;
      if (currentFileSize > maxFileSize) {
        out.close();
        out = null;

        if (renameAtEnd) {
          long renameStart = System.currentTimeMillis();
          fs.rename(currentPath, new Path(currentPath.toString() + ".renamed"));
          long renameEnd = System.currentTimeMillis();
          renameTime += (renameEnd - renameStart);
          totalRenames++;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to write");
    }
  }

  @Override public void endWindow()
  {
    try {
      if (out != null && flushEnabled) {
        out.flush();
        out.hflush();
        out.hsync();
      }
      super.endWindow();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override public void checkpointed(long windowId)
  {
    long timeDiff = System.currentTimeMillis() - start;
    logger.info("Wrote {} bytes in {} seconds speed {} Mb/sec", totalWritten, timeDiff,
        (totalWritten * 1000.0/(timeDiff * 1024 * 1024.0)));
    if (renameAtEnd && totalRenames > 0) {
      logger.info("Rename time {} total Renames {} avg {}", renameTime, totalRenames, (float) renameTime / totalRenames);
    }
    start = System.currentTimeMillis();
    totalWritten = 0;
  }

  @Override public void committed(long windowId)
  {

  }

  private static final Logger logger = LoggerFactory.getLogger(FsWriteOperator.class);
}
