package io.dazzleduck.sql.spark;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.server.Main;
import org.apache.arrow.flight.FlightServer;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

public class DuckLakeSparkIntegrationTest {

    private static SparkSession spark;
    private static Path workspace;
    private static FlightServer server;

    private static final int PORT = 33336;
    private static final String USER = "admin";
    private static final String PASSWORD = "admin";

    private static final String CATALOG = "test_ducklake";
    private static final String SCHEMA = "main";
    private static final String TABLE = "tt_p";

    private static final String RPC_TABLE = "rpc_tt_p";
    private static final String URL = "jdbc:arrow-flight-sql://localhost:" + PORT + "?useEncryption=false&disableCertificateVerification=true&disableSessionCatalog=true" + "&user=" + USER + "&password=" + PASSWORD;
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

        server = Main.createServer(new String[]{
                "--conf", "dazzleduck_server.flight_sql.port=" + PORT,
                "--conf", "dazzleduck_server.flight_sql.use_encryption=false",
                "--conf", "dazzleduck_server.access_mode=RESTRICTED"
        });
        server.start();

        createDuckLakeRPCTable(SCHEMA_DDL, RPC_TABLE, CATALOG, SCHEMA, TABLE);
    }
    private static void createDuckLakeRPCTable(String schemaDDL, String viewName, String catalog, String schema, String table) {

        String sql = """
        CREATE TEMP VIEW %s (%s)
        USING %s
        OPTIONS (
          url '%s',
          database '%s',
          schema '%s',
          table '%s',
          username '%s',
          password '%s',
          connection_timeout 'PT10M'
        )
        """.formatted(viewName, schemaDDL, ArrowRPCTableProvider.class.getName(), URL,
                catalog, schema, table, USER, PASSWORD);

        spark.sql(sql);
    }
    @Test
    void testSparkClientDucklakeTable() {
        long sparkCount = spark.sql(String.format("SELECT count(*) FROM %s", RPC_TABLE)).first().getLong(0);
        Assertions.assertEquals(4, sparkCount);
        Assertions.assertTrue(ConnectionPool.execute("SELECT COUNT(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE)));
    }

    @Test
    void testDuckLakeTableExists() {
        var result = ConnectionPool.execute("SELECT COUNT(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE));
        Assertions.assertTrue(result);
    }

    @Test
    void testSimpleFilterByKey() {

        long sparkCount = spark.sql(String.format("SELECT count(*) FROM %s WHERE key = 'k51'", RPC_TABLE)).first().getLong(0);
        Assertions.assertEquals(1, sparkCount);
        var result = spark.sql(String.format("SELECT key, value, partition FROM %s WHERE key = 'k51'", RPC_TABLE)).first();

        Assertions.assertEquals("k51", result.getString(0));
        Assertions.assertEquals("v51", result.getString(1));
        Assertions.assertEquals(1, result.getInt(2));
    }

    @Test
    void testFilterByPartition() {

        long partition0Count = spark.sql(String.format("SELECT count(*) FROM %s WHERE partition = 0", RPC_TABLE)).first().getLong(0);
        Assertions.assertEquals(2, partition0Count);
        long partition1Count = spark.sql(String.format("SELECT count(*) FROM %s WHERE partition = 1", RPC_TABLE)).first().getLong(0);
        Assertions.assertEquals(2, partition1Count);
    }

    @Test
    void testMultipleConditions() {
        var result = spark.sql(String.format("SELECT * FROM %s WHERE key = 'k51' AND partition = 1", RPC_TABLE)).collectAsList();

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("k51", result.get(0).getString(0));
        Assertions.assertEquals("v51", result.get(0).getString(1));
        Assertions.assertEquals(1, result.get(0).getInt(2));
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (spark != null) {
            spark.close();
        }
        if (server != null) {
            server.close();
        }
        ConnectionPool.execute("DETACH " + CATALOG);
    }
}
