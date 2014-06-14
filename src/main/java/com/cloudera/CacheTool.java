/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CacheTool {

  static Configuration conf;
  static HdfsAdmin admin;

  static ExecutorService pool = Executors.newFixedThreadPool(10);

  public static void createFile(FileSystem fs, Path fileName, long fileLen)
      throws IOException {
    int bufferLen = 1024;
    assert bufferLen > 0;
    if (!fs.mkdirs(fileName.getParent())) {
      throw new IOException("Mkdirs failed to create " +
          fileName.getParent().toString());
    }
    FSDataOutputStream out = null;
    try {
      out = fs.create(fileName, true, fs.getConf()
              .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
          (short) 1, fs.getDefaultBlockSize(fileName));
      if (fileLen > 0) {
        byte[] toWrite = new byte[bufferLen];
        Random rb = new Random(0);
        long bytesToWrite = fileLen;
        while (bytesToWrite>0) {
          rb.nextBytes(toWrite);
          int bytesToWriteNext = (bufferLen < bytesToWrite) ? bufferLen
              : (int) bytesToWrite;

          out.write(toWrite, 0, bytesToWriteNext);
          bytesToWrite -= bytesToWriteNext;
        }
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    conf = new Configuration();
    conf.addResource(new Path("/home/james/hdfs-conf/hdfs-site.xml"));
    conf.addResource(new Path("/home/james/hdfs-conf/core-site.xml"));
    URI uri = FileSystem.getDefaultUri(conf);
    final FileSystem fs =
      FileSystem.get(uri, conf);

    for (int i = 0; i < 8000; i += 10) {
      final int i_copy = i;
      pool.submit(new Runnable() {
        public void run() {
          for (int j = 0; j < 10; j++) {
            try {
              createFile(fs, new Path("/home/james/large" + (i_copy + j)), 1024 * 1024);
            } catch (IOException ioe) {
              System.out.println(ioe);
            }
          }
        }
      });
    }
    pool.shutdown();
    pool.awaitTermination(1, TimeUnit.DAYS);


    long start = Time.monotonicNow();
    Random r = new Random(0);
    for (int i = 0; i < 100; i++) {
      FSDataInputStream fdis = fs.open(new Path("/home/james/large" + r.nextInt(
          8000)), 512);
      byte[] buffer = new byte[512];

      for (int j = 0; j < 100; j++) {
        int offset = r.nextInt(1024 * 1024 - 511);
        fdis.read(offset, buffer, 0, 512);
      }
    }
    System.out.println("Time taken for 10000 random 512 byte reads: " +
        (Time.monotonicNow() - start) / 1000.0);

  }
}
