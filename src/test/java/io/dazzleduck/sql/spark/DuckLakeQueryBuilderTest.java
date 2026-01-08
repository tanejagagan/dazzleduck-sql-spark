package io.dazzleduck.sql.spark;

import io.dazzleduck.sql.spark.expression.LiteralValue;
import io.dazzleduck.sql.spark.extension.FieldReference;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DuckLakeQueryBuilderTest {

    @Test
    public void testDuckLakeWithPartitionFilter() throws SQLException, IOException {
        var datasourceSchema = (StructType) DataType.fromDDL("key string, value string, p int");
        var partitionSchema = (StructType) DataType.fromDDL("p int");

        var datasourceOptions = new DatasourceOptions(
                "localhost",
                null,
                "example",
                List.of("p"),
                Duration.ofMinutes(10),
                new Properties(),
                DatasourceOptions.SourceType.DUCKLAKE,
                "my_data",    // catalog
                "data",    // schema
                "kv"       // table
        );

        var outputSchema = (StructType) DataType.fromDDL("key string, value string");

        // Filter: p = 1
        var predicateChildren = new Expression[] {
                new LiteralValue<>(1, DataType.fromDDL("int")),
                new FieldReference(new String[]{"p"})
        };
        var predicates = new Predicate[]{
                new Predicate("=", predicateChildren)
        };

        var sql = QueryBuilderV2.build(
                datasourceSchema,
                partitionSchema,
                datasourceOptions,
                outputSchema,
                predicates,
                10,
                new DuckDBExpressionSQLBuilder(datasourceSchema)
        );

        // Define the expected SQL string
        String expectedSql = "SELECT \"key\", \"value\" FROM \n" +
                "(FROM (VALUES(NULL::varchar,NULL::varchar,NULL::int)) t(\"key\", \"value\", \"p\")\n" +
                "WHERE false\n" +
                "UNION ALL BY NAME\n" +
                "FROM my_data.data.kv) \n" +
                "WHERE 1 = \"p\" \n" +
                "limit 10";

        // Use assert instead of sout
        assertEquals(expectedSql.trim(), sql.trim(), "The generated SQL does not match the expected output.");
    }
}