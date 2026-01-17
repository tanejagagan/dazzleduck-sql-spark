package io.dazzleduck.sql.spark;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.flight.server.Main;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;

import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkFsArrowRPCTest {

    private static final Logger logger = LoggerFactory.getLogger(SparkFsArrowRPCTest.class);

    private static final int PORT = 33334;
    private static final String URL = String.format(
            "jdbc:arrow-flight-sql://localhost:%s?useEncryption=false&disableCertificateVerification=true&user=admin&password=admin",
            PORT);

    private static final String LOCAL_TABLE = "table1";
    private static final String S3_TABLE = "s3_table1";
    private static final String RPC_LOCAL_PATH_TABLE = "rpc_local_path_table";
    private static final String RPC_S3_PATH_TABLE = "rpc_s3_path_table";
    private static final String SCHEMA_DDL = "time timestamp, key string, value string, quantity bigint, size int, price decimal(10,3), s struct<i1 int>, partition int";

    private static final String SCHEMA_EVOLUTION_RPC_TABLE = "schema_evolution_rpc";
    private static final String SCHEMA_EVOLUTION_LOCAL_TABLE = "schema_evolution_local";
    private static final String SCHEMA_EVOLUTION_DDL = "key string, value string, nested struct< a : int, b : int, c : int>, array array<int>, p int";

    private static final Network network = Network.newNetwork();
    private static final MinIOContainer minio = MinioContainerTestUtil.createContainer("minio", network);

    private static SparkSession spark;
    private static MinioClient minioClient;
    private static FlightServer server;
    private static String localPath;
    private static String s3Path;
    private static String schemaEvolutionTablePath;


    private static void createLocalTable(String schemaDDL, String tableName, String path) {
        spark.sql(String.format("CREATE TEMPORARY VIEW %s (%s) USING parquet OPTIONS (path '%s')", tableName, schemaDDL, path));
    }

    @BeforeAll
    public static void setup() throws Exception {
        minio.start();
        minioClient = MinioContainerTestUtil.createClient(minio);
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(MinioContainerTestUtil.TEST_BUCKET_NAME).build());

        localPath = Paths.get(System.getProperty("user.dir"), "example", "data", "parquet", "spark_fs_test").toString();
        schemaEvolutionTablePath = Paths.get(System.getProperty("user.dir"), "example", "data", "parquet", "kv").toUri().toString();
        MinioContainerTestUtil.uploadDirectory(minioClient, MinioContainerTestUtil.TEST_BUCKET_NAME, localPath, "spark_fs_test/");
        s3Path = String.format("s3a://%s/%s", MinioContainerTestUtil.TEST_BUCKET_NAME, "spark_fs_test");

        var secretConfig = MinioContainerTestUtil.duckDBSecretForS3Access(minio).entrySet().stream()
                .map(e -> "{key = " + e.getKey() + ", value = \"" + e.getValue() + "\"}")
                .collect(Collectors.joining(","));
        var secretsConfigStr = String.format("secrets = { sec1 = [%s]}", secretConfig);
        var secretsConfig = ConfigFactory.parseString(secretsConfigStr);
        var config = ConfigFactory.load();
        var configWithFallback = config.withFallback(secretsConfig);

        spark = SparkInitializationHelper.createSparkSession(configWithFallback);
        DuckDBInitializationHelper.initializeDuckDB(configWithFallback);

        server = Main.createServer(new String[]{
                "--conf", "dazzleduck_server.flight_sql.port=" + PORT,
                "--conf", "dazzleduck_server.flight_sql.use_encryption=false",
                "--conf", "dazzleduck_server.access_mode=RESTRICTED"
        });
        server.start();

        String sparkPath = Paths.get(localPath).toUri().toString();
        createLocalTable(SCHEMA_DDL, LOCAL_TABLE, sparkPath);
        createLocalTable(SCHEMA_DDL, S3_TABLE, s3Path);
        createLocalTable(SCHEMA_EVOLUTION_DDL, SCHEMA_EVOLUTION_LOCAL_TABLE, schemaEvolutionTablePath);

        createRPCTestTable(SCHEMA_DDL, RPC_LOCAL_PATH_TABLE, sparkPath, "partition");
        createRPCTestTable(SCHEMA_DDL, RPC_S3_PATH_TABLE, s3Path, "partition");
        createRPCTestTable(SCHEMA_EVOLUTION_DDL, SCHEMA_EVOLUTION_RPC_TABLE, schemaEvolutionTablePath, "p");
    }

    private static void createRPCTestTable(String schema, String name, String path, String partitionColumns) {
        String createSql = String.format("""
                CREATE TEMP VIEW %s (%s)
                USING %s
                OPTIONS (
                    url '%s',
                    path '%s',
                    function 'read_parquet',
                    username 'admin',
                    password 'admin',
                    partition_columns '%s',
                    connection_timeout 'PT10M',
                    parallelize 'true'
                )""", name, schema, ArrowRPCTableProvider.class.getName(), URL, path, partitionColumns);
        spark.sql(createSql);
    }

    public static String[] getTestSQLs() {
        return new String[]{
                "select * from %s order by key",
                "select s.i1 from %s order by key",
                "select count(*) from %s",
                "select count(*) from %s where partition = 100",
                "select count(s.i1) from %s where partition = 100",
                "select *, s.i1 from %s order by key",
                "select partition from %s where partition = 1",
                "select * from %s where partition = 1",
                "select * from %s where key  = 'k1'",
                "select * from %s where s.i1 = 1",
                "select count(s.i1), s.i1 from %s group by s.i1 order by s.i1",
                "select count(*) from %s group by s.i1 order by s.i1",
                "select count(*), s.i1 from %s group by s.i1 order by s.i1",
                "select count(*), sum(quantity), sum(size), sum(price), min(quantity), max(quantity), partition from %s where partition = 1 group by partition",
                "select count(*), sum(quantity), sum(size), sum(price), avg(price), min(quantity), max(quantity), partition from %s where partition = 1 group by partition",
                "select count(*), to_date(time) from %s group by to_date(time) order by to_date(time)",
                "select count(*), month(time), year(time) from %s group by month(time), year(time), second(time) order by month(time)",
                "select count(*), sum(quantity), sum(size), sum(price), avg(price), min(quantity), max(quantity), partition + 2 from %s where partition = 1 group by partition + 2",
                "select count(*), sum(quantity), sum(size), sum(price), avg(price), min(quantity), max(quantity), date_trunc('day', time) from %s where partition = 1 group by date_trunc( 'day', time)",
        };
    }

    public static String[] getTestSchemaEvolutionSQLs() {
        return new String[]{
                "select p, array from %s order by p",
                "select count(*) from %s where nested.c is null order by count(*)"
        };
    }

    @ParameterizedTest
    @MethodSource("getTestSQLs")
    void testSql(String sql) {
        test(sql, LOCAL_TABLE, RPC_LOCAL_PATH_TABLE);
    }

    @ParameterizedTest
    @MethodSource("getTestSchemaEvolutionSQLs")
    void testSchemaEvolutionSql(String sql) {
        test(sql, SCHEMA_EVOLUTION_LOCAL_TABLE, SCHEMA_EVOLUTION_RPC_TABLE);
    }

    @Test
    void testPathTable() {
        var sql = "select * from %s order by key";
        test(sql, LOCAL_TABLE, RPC_LOCAL_PATH_TABLE);
    }

    @Test
    void testS3Table() {
        var sql = "select * from %s order by key";
        test(sql, S3_TABLE, RPC_S3_PATH_TABLE);
    }

    private void test(String sql, String expectedTable, String resultTable) {
        String  expectedTableSql = String.format(sql, expectedTable);
        String  replaceTableSql = String.format(sql, resultTable);
        SparkTestHelper.assertEqual(spark, expectedTableSql, replaceTableSql);
    }

    @Test
    void testRPCScanRestricted() {
        var flightClient = FlightClient.builder()
                .allocator(new RootAllocator())
                .location(Location.forGrpcInsecure("localhost", PORT))
                .intercept(AuthUtils.createClientMiddlewareFactory("admin", "admin",
                        Map.of("path", "example/hive_table", "function", "read_parquet")))
                .build();

        var flightSqlClient = new FlightSqlClient(flightClient);
        flightSqlClient.execute("SELECT * FROM read_parquet('example/hive_table/*/*/*.parquet')");
    }

    @Test
    void testConnectionPool() throws Exception {
        var options = DatasourceOptions.parse(Map.of(
                "url", URL,
                "connection_timeout", "PT10M",
                "username", "admin",
                "password", "admin",
                "path", localPath,
                "function", "read_parquet",
                "parallelize", "true"
        ));

        var headers = new FlightCallHeaders();
        headers.insert(Headers.HEADER_PATH, localPath);
        headers.insert(Headers.HEADER_FUNCTION, "read_parquet");
        var schemaOption = new HeaderCallOption(headers);

        var info = FlightSqlClientPool.INSTANCE.getInfo(
                options, "SELECT * FROM read_parquet('" + localPath + "/**/*.parquet')", schemaOption);

        Assertions.assertNotNull(info);
        try (var stream = FlightSqlClientPool.INSTANCE.getStream(options, info.getEndpoints().get(0))) {
            var root = stream.getRoot();
            int batchCount = 0;
            while (stream.next()) {
                batchCount++;
                logger.debug("Batch {}: {}", batchCount, root.contentToTSVString());
            }
        }
    }

    @AfterAll
    public static void stopAll() throws Exception {
        if (spark != null) {
            spark.close();
        }
        if (server != null) {
            server.close();
        }
        if (minio != null && minio.isRunning()) {
            minio.stop();
        }
    }
}


