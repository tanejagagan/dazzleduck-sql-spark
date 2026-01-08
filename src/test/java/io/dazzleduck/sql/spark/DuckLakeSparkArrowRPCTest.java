package io.dazzleduck.sql.spark;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DuckLakeSparkArrowRPCTest {

    private static SparkSession spark;
    private static Path workspace;

    private static final int PORT = 33335;
    private static final String USER = "admin";
    private static final String PASSWORD = "admin";

    private static final String CATALOG = "test_ducklake";
    private static final String SCHEMA = "main";
    private static final String TABLE = "tt_p";

    private static final String RPC_TABLE = "rpc_tt_p";
    private static final String URL = "jdbc:arrow-flight-sql://localhost:" + PORT + "?useEncryption=false&disableCertificateVerification=true" + "&user=" + USER + "&password=" + PASSWORD;
    private static final String SCHEMA_DDL = "key string, value string, partition int";

    @BeforeAll
    static void setup() throws Exception {

        workspace = Files.createTempDirectory("ducklake_rpc_test_");

        ConnectionPool.executeBatch(new String[]{
                "INSTALL ducklake",
                "LOAD ducklake",
                "ATTACH 'ducklake:%s/metadata' AS %s (DATA_PATH '%s/data')".formatted(workspace, CATALOG, workspace)
        });

        ConnectionPool.execute("""
            CREATE TABLE %s.%s.%s (
              key string,
              value string,
              partition int )
            """.formatted(CATALOG, SCHEMA, TABLE));

        ConnectionPool.execute("""
            ALTER TABLE %s.%s.%s
            SET PARTITIONED BY (partition)
            """.formatted(CATALOG, SCHEMA, TABLE));

        ConnectionPool.execute("""
            INSERT INTO %s.%s.%s VALUES
              ('k00','v00',0),
              ('k01','v01',0),
              ('k51','v51',1),
              ('k61','v61',1)
            """.formatted(CATALOG, SCHEMA, TABLE));

        var config = ConfigFactory.load();

        spark = SparkInitializationHelper.createSparkSession(config);
        DuckDBInitializationHelper.initializeDuckDB(config);
        FlightTestUtil.createFsServiceAnsStart(PORT);
        createDuckLakeRPCTable(SCHEMA_DDL, RPC_TABLE, CATALOG, SCHEMA, TABLE);
    }
    private static void createDuckLakeRPCTable(String schemaDDL, String viewName, String catalog, String schema, String table) {

        String identifier = catalog + "." + schema + "." + table;

        String sql = """
        CREATE TEMP VIEW %s (%s)
        USING %s
        OPTIONS (
          url '%s',
          identifier '%s',
          catalog '%s',
          schema '%s',
          table '%s',
          username '%s',
          password '%s',
          partition_columns 'partition',
          connection_timeout 'PT10M'
        )
        """.formatted(viewName, schemaDDL, ArrowRPCTableProvider.class.getName(), URL, identifier,
                catalog, schema, table, USER, PASSWORD);

        spark.sql(sql);
        spark.sql(String.format("select * from %s", viewName)).show();

    }
    @Test
    void SparkClientDucklakeTable() {
        long sparkCount = spark.sql(String.format("SELECT count(*) FROM %s", RPC_TABLE)).first().getLong(0);
        ConnectionPool.printResult("SELECT count(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE));
        Assertions.assertEquals(4, sparkCount);
        Assertions.assertTrue(ConnectionPool.execute("SELECT COUNT(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE)));
    }

    @Test
    void DuckLakeTableExists() {
        var result = ConnectionPool.execute(" SELECT COUNT(*) FROM %s.%s.%s ".formatted(CATALOG, SCHEMA, TABLE));
        System.out.println(result);
        ConnectionPool.printResult("SELECT count(*) FROM test_ducklake.main.tt_p");
    }

    @Test
    void testFlightSqlDirectly() {
        var client = FlightClient.builder()
                .allocator(new RootAllocator())
                .location(Location.forGrpcInsecure("localhost", PORT))
                .intercept(AuthUtils.createClientMiddlewareFactory(USER, PASSWORD, Map.of()))
                .build();

        var sqlClient = new FlightSqlClient(client);
        var info = sqlClient.execute("select 1");
        Assertions.assertFalse(info.getEndpoints().isEmpty());
    }

    @AfterAll
    static void cleanup() {
        spark.close();
        ConnectionPool.execute("DETACH " + CATALOG);
    }
}
