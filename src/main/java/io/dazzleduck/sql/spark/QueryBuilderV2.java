package io.dazzleduck.sql.spark;

import io.dazzleduck.sql.spark.extension.FieldReference;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.types.StructType;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.stream;

/**
 * The supported format is as following
 * Aggregation
 * select count(*), key from (( inner_query_with_types) where filters) group by key  limit 10;
 * <p>
 * Projection Only
 * select c1, c2 from ((inner_query_with_types) where filters) limit 10;
 * <p>
 * inner_query_with_types
 */
public class QueryBuilderV2 {
    public static String build(StructType datasourceSchema,
                               StructType partitionSchema,
                               DatasourceOptions datasourceOptions,
                               StructType outputSchema,
                               Expression[] pushedPredicates,
                               int limit,
                               DuckDBExpressionSQLBuilder dialect) {
        var source = buildSource(datasourceOptions, partitionSchema);
        var inner = DuckDBExpressionSQLBuilder.buildCast(datasourceSchema, source, dialect);
        var selectClause = stream(outputSchema.fieldNames()).map(f -> dialect.build(new FieldReference(new String[]{f}))).collect(Collectors.joining(", "));
        var whereClause = pushedPredicates == null || pushedPredicates.length == 0 ?
                "" : "WHERE " + stream(pushedPredicates).map(dialect::build).collect(Collectors.joining(" AND "));
        var limitClause = limit < 0 ? "" : "limit %s".formatted(limit);
        return "SELECT %s FROM \n(%s) \n%s \n%s".formatted(selectClause, inner, whereClause, limitClause);
    }

    public static String buildForAggregation(StructType datasourceSchema,
                                             StructType partitionSchema,
                                             DatasourceOptions datasourceOptions,
                                             Expression[] pushedPredicates,
                                             Aggregation pushedAggregation,
                                             int limit,
                                             DuckDBExpressionSQLBuilder dialect) {
        var source = buildSource(datasourceOptions, partitionSchema);
        var inner = DuckDBExpressionSQLBuilder.buildCast(datasourceSchema, source, dialect);
        var selectClause = Stream.concat(stream(pushedAggregation.groupByExpressions()), stream(pushedAggregation.aggregateExpressions())).map(dialect::build).collect(Collectors.joining(","));
        var whereClause = pushedPredicates == null || pushedPredicates.length == 0 ?
                "" : "WHERE " + stream(pushedPredicates).map(dialect::build).collect(Collectors.joining(" AND "));
        var groupByClause = pushedAggregation.groupByExpressions() == null || pushedAggregation.groupByExpressions().length == 0 ?
                "" : "GROUP BY " + stream(pushedAggregation.groupByExpressions()).map(dialect::build).collect(Collectors.joining(", "));
        var limitClause = limit < 0 ? "" : "limit %s".formatted(limit);
        return "SELECT %s FROM \n(%s) \n%s \n%s \n%s".formatted(selectClause, inner, whereClause, groupByClause, limitClause);
    }

    private static String buildSource(DatasourceOptions options, StructType partitionSchema) {

    /* ===============================
       DuckLake: table-based
       =============================== */
        if (options.sourceType() == DatasourceOptions.SourceType.DUCKLAKE) {
            return "%s.%s.%s".formatted(options.catalog(), options.schema(), options.table());
        }

    /* ===============================
       Hive: file-based
       =============================== */
        var path = options.path();

        if (options.partitionColumns().isEmpty()) {
            return "read_parquet('%s')".formatted(path);
        }

        String partition = "/*".repeat(options.partitionColumns().size()) + "/*.parquet";

        var hiveTypes = stream(partitionSchema.fields())
                        .map(f -> "%s:%s".formatted(f.name(), DuckDBExpressionSQLBuilder.translateDataType(f.dataType())))
                        .collect(Collectors.joining(","));

        return "read_parquet('%s%s', hive_types={%s}, union_by_name=true)".formatted(path, partition, hiveTypes);
    }
}