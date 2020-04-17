package com.hortonworks.spark.sql.hive.llap.writers;

import com.hortonworks.spark.sql.hive.llap.util.SparkToHiveRecordMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.orc.OrcOutputWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HiveWarehouseDataWriter implements DataWriter<InternalRow> {
  private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseDataWriter.class);

  private String jobId;
  private StructType schema;
  private int partitionId;
  private int attemptNumber;
  private FileSystem fs;
  private Path filePath;
  private OutputWriter out;
  private SparkToHiveRecordMapper sparkToHiveRecordMapper;

  public HiveWarehouseDataWriter(Configuration conf, String jobId, StructType schema,
      int partitionId, int attemptNumber, FileSystem fs, Path filePath, SparkToHiveRecordMapper sparkToHiveRecordMapper) {
    this.jobId = jobId;
    this.schema = schema;
    this.partitionId = partitionId;
    this.attemptNumber = attemptNumber;
    this.fs = fs;
    this.filePath = filePath;
    this.sparkToHiveRecordMapper = sparkToHiveRecordMapper;
    conf.set("orc.mapred.output.schema", sparkToHiveRecordMapper.getSchemaInHiveColumnsOrder().catalogString());
    TaskAttemptContext tac = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    this.out = getOutputWriter(filePath.toString(), sparkToHiveRecordMapper.getSchemaInHiveColumnsOrder(), tac);
    logInfo("CREATED...");

  }

  @Override public void write(InternalRow record) throws IOException {
    out.write(sparkToHiveRecordMapper.mapToHiveColumns(record));
  }

  @Override public WriterCommitMessage commit() throws IOException {
    out.close();
    logInfo("COMMITTING...");
    return new SimpleWriterCommitMessage(String.format("COMMIT %s_%s_%s", jobId, partitionId, attemptNumber), filePath);
  }

  @Override public void abort() throws IOException {
    LOG.info("Driver sent abort for {}_{}_{}", jobId, partitionId, attemptNumber);
    logInfo("ABORT RECEIVED FROM DRIVER...");
    try {
      out.close();
    } finally {
      fs.delete(filePath, false);
    }
  }

  protected OutputWriter getOutputWriter(String path, StructType schema, TaskAttemptContext tac) {
    return new OrcOutputWriter(path, schema, tac);
  }

  private void logInfo(String msg) {
    LOG.info("HiveWarehouseDataWriter: {}, path: {}, msg:{} ", this, filePath, msg);
  }
}
