package org.apache.hudi.utilities.sources.debezium;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.DataTypes;

/**
 * Source for incrementally ingesting Debezium generated change logs for Oracle DB.
 */
public class OracleDebeziumSource extends DebeziumSource {

  private final SQLContext sqlContext;
  private final String jdbcUrl;
  private final String jdbcUsername;
  private final String jdbcPassword;

  public OracleDebeziumSource(TypedProperties props, JavaSparkContext sparkContext,
                              SparkSession sparkSession,
                              SchemaProvider schemaProvider,
                              HoodieIngestionMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider, metrics);
    this.sqlContext = sparkSession.sqlContext();

    // Read Oracle connection details from config
    this.jdbcUrl = props.getString("hoodie.datasource.oracle.jdbc.url");
    this.jdbcUsername = props.getString("hoodie.datasource.oracle.jdbc.username");
    this.jdbcPassword = props.getString("hoodie.datasource.oracle.jdbc.password");
  }

  @Override
  protected Dataset<Row> fetchData(String debeziumTopic) throws Exception {
    // Connect to Oracle database using JDBC
    Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
    Statement statement = connection.createStatement();

    // Replace with your actual Debezium schema name and table name
    String query = "SELECT * FROM <SCHEMA_NAME>.<DEBZEIUM_CHANGE_LOG_TABLE> " +
                   "WHERE dml_op IN ('I', 'U', 'D')";

    ResultSet resultSet = statement.executeQuery(query);

    // Convert ResultSet to Spark DataFrame
    Dataset<Row> dataset = sqlContext.createDataFrame(resultSet, inferSchema(resultSet));

    // Close resources
    resultSet.close();
    statement.close();
    connection.close();

    return dataset;
  }

  // Utility method to infer schema from ResultSet (optional)
  private static org.apache.spark.sql.schema.StructType inferSchema(ResultSet resultSet) throws Exception {
    org.apache.spark.sql.schema.StructType schema = new org.apache.spark.sql.schema.StructType();
    int numColumns = resultSet.getMetaData().getColumnCount();
    for (int i = 1; i <= numColumns; i++) {
      String columnName = resultSet.getMetaData().getColumnName(i);
      String columnType = resultSet.getMetaData().getColumnTypeName(i);
      schema = schema.add(columnName, DataTypes.createDataType(columnType), false);
    }
    return schema;
  }

  // Override processDataset method if needed for Oracle specific transformations

  @Override
  protected Dataset<Row> processDataset(Dataset<Row> rowDataset) {
    // You can implement custom logic for processing Oracle data here
    return super.processDataset(rowDataset);
  }

  // Including Oracle JDBC Driver

  // You need to include the Oracle JDBC driver in your classpath for the code to connect to the database.
  // There are several ways to achieve this:
  // 1. Download the driver JAR file (e.g., ojdbc8.jar) and add it to your project's libraries.
  // 2. Use a dependency management tool like Maven or Gradle to include the driver as a dependency in your project.
  // 3. If you're using a cluster environment, the driver might already be available on the classpath. Check with your cluster administrator.
}
