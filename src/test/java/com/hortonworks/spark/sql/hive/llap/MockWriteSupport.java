package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.util.SerializableHadoopConfiguration;
import com.hortonworks.spark.sql.hive.llap.util.SparkToHiveRecordMapper;
import com.hortonworks.spark.sql.hive.llap.writers.HiveWarehouseDataSourceWriter;
import com.hortonworks.spark.sql.hive.llap.writers.HiveWarehouseDataWriter;
import com.hortonworks.spark.sql.hive.llap.writers.HiveWarehouseDataWriterFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.orc.OrcOutputWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MockWriteSupport {

  public static class MockHiveWarehouseDataSourceWriter extends HiveWarehouseDataSourceWriter {

    public MockHiveWarehouseDataSourceWriter(Map<String, String> options, String jobId, StructType schema, Path path,
                                             Configuration conf, SaveMode mode) {
      super(options, jobId, schema, path, conf, mode);
    }

    @Override
    public DataWriterFactory<InternalRow> createInternalRowWriterFactory() {
      return new MockHiveWarehouseDataWriterFactory(jobId, schema, path, new SerializableHadoopConfiguration(conf));
    }

    @Override public void commit(WriterCommitMessage[] messages) {

    }
  }

  public static class MockHiveWarehouseDataWriterFactory extends HiveWarehouseDataWriterFactory {

    public MockHiveWarehouseDataWriterFactory(String jobId, StructType schema, Path path, SerializableHadoopConfiguration conf) {
      super(jobId, schema, path, conf, new SparkToHiveRecordMapper(schema, null));
    }

    protected DataWriter<InternalRow> getDataWriter(Configuration conf, String jobId,
        StructType schema, int partitionId, int attemptNumber,
        FileSystem fs, Path filePath, SparkToHiveRecordMapper sparkToHiveRecordMapper) {
      return new MockHiveWarehouseDataWriter(conf, jobId, schema, partitionId, attemptNumber, fs, filePath, sparkToHiveRecordMapper);
    }

  }

  public static class MockHiveWarehouseDataWriter extends HiveWarehouseDataWriter {

    public MockHiveWarehouseDataWriter(Configuration conf, String jobId, StructType schema, int partitionId,
        int attemptNumber, FileSystem fs, Path filePath, SparkToHiveRecordMapper sparkToHiveRecordMapper) {
      super(conf, jobId, schema, partitionId, attemptNumber, fs, filePath, sparkToHiveRecordMapper);
    }

    @Override
    protected OutputWriter getOutputWriter(String path, StructType schema, TaskAttemptContext tac) {
      return new MockOutputWriter(path, schema, tac);
    }

  }

  public static class MockOutputWriter extends OutputWriter {

    public static List<InternalRow> rowBuffer = new ArrayList<>();
    public static boolean closed = false;

    public MockOutputWriter(String path, StructType schema, TaskAttemptContext tac) {
    }

    @Override public void write(InternalRow row) {
      rowBuffer.add(row);
    }

    @Override public void close() {
      MockHiveWarehouseConnector.writeOutputBuffer.put("TestWriteSupport", rowBuffer);
    }
  }

}