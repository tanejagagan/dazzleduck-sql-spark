package io.dazzleduck.sql.spark;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.flight.server.Main;
import org.apache.arrow.driver.jdbc.ArrowFlightConnection;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.junit.jupiter.api.*;

import java.nio.file.Paths;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for FlightSqlClientPool connection pooling behavior.
 */
public class FlightSqlClientPoolTest {

    private static final int PORT = 33335;
    private static final String URL = String.format(
            "jdbc:arrow-flight-sql://localhost:%s?useEncryption=false&disableCertificateVerification=true&user=admin&password=admin",
            PORT);
    private static String testPath;
    private static FlightServer server;

    @BeforeAll
    public static void setup() throws Exception {
        testPath = Paths.get(System.getProperty("user.dir"), "example", "data", "parquet", "spark_fs_test").toString();
        var config = ConfigFactory.load();
        DuckDBInitializationHelper.initializeDuckDB(config);

        server = Main.createServer(new String[]{
                "--conf", "dazzleduck_server.flight_sql.port=" + PORT,
                "--conf", "dazzleduck_server.flight_sql.use_encryption=false",
                "--conf", "dazzleduck_server.access_mode=RESTRICTED"
        });
        server.start();
    }

    @AfterAll
    public static void cleanup() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @BeforeEach
    public void clearCache() throws Exception {
        // Clear the cache before each test to ensure isolation
        var cacheField = FlightSqlClientPool.class.getDeclaredField("cache");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        var cache = (Map<String, ?>) cacheField.get(FlightSqlClientPool.INSTANCE);
        cache.clear();
    }

    /**
     * Test that multiple calls with the same options reuse the same connection.
     */
    @Test
    void testConnectionReuse() throws Exception {
        var options = createOptions("PT10M"); // 10 minute timeout

        // First call - creates a new connection
        FlightInfo info1 = FlightSqlClientPool.INSTANCE.getInfo(options, buildQuery(testPath), createHeaders(testPath));
        assertNotNull(info1);

        // Get the cached client
        FlightSqlClient client1 = getCachedClient(options);
        assertNotNull(client1);

        // Second call - should reuse the same connection
        FlightInfo info2 = FlightSqlClientPool.INSTANCE.getInfo(options, buildQuery(testPath), createHeaders(testPath));
        assertNotNull(info2);

        // Get the cached client again
        FlightSqlClient client2 = getCachedClient(options);

        // Verify same client instance is reused
        assertSame(client1, client2, "Connection should be reused for subsequent calls");
    }

    /**
     * Test that expired connections are replaced with new ones.
     */
    @Test
    void testConnectionExpiration() throws Exception {
        // Use a very short timeout (1 millisecond)
        var options = createOptions("PT0.001S");

        // First call - creates a new connection
        FlightSqlClientPool.INSTANCE.getInfo(options, buildQuery(testPath), createHeaders(testPath));
        FlightSqlClient client1 = getCachedClient(options);
        long timestamp1 = getCachedTimestamp(options);

        // Wait for the connection to expire
        Thread.sleep(50);

        // Second call - should create a new connection since the old one expired
        FlightSqlClientPool.INSTANCE.getInfo(options, buildQuery(testPath), createHeaders(testPath));
        FlightSqlClient client2 = getCachedClient(options);
        long timestamp2 = getCachedTimestamp(options);

        // Verify a new client was created
        assertNotSame(client1, client2, "A new connection should be created after expiration");
        assertTrue(timestamp2 > timestamp1, "New connection should have a later timestamp");
    }

    /**
     * Test that different options result in different cached connections.
     */
    @Test
    void testDifferentOptionsGetDifferentConnections() throws Exception {
        String path1 = testPath;
        String path2 = Paths.get(System.getProperty("user.dir"), "example", "data", "parquet", "kv").toString();

        var options1 = createOptionsWithPath("PT10M", path1);
        var options2 = createOptionsWithPath("PT10M", path2);

        // Create connections for both options
        FlightSqlClientPool.INSTANCE.getInfo(options1, buildQuery(path1), createHeaders(path1));
        FlightSqlClientPool.INSTANCE.getInfo(options2, buildQuery(path2), createHeaders(path2));

        FlightSqlClient client1 = getCachedClient(options1);
        FlightSqlClient client2 = getCachedClient(options2);

        // Verify different clients for different options
        assertNotSame(client1, client2, "Different options should have different connections");

        // Verify cache has two entries
        assertEquals(2, getCacheSize(), "Cache should have two entries for different options");
    }

    /**
     * Test that the connection is still valid after reuse.
     */
    @Test
    void testConnectionRemainsValidAfterReuse() throws Exception {
        var options = createOptions("PT10M");

        // Make multiple calls and verify they all succeed
        for (int i = 0; i < 5; i++) {
            FlightInfo info = FlightSqlClientPool.INSTANCE.getInfo(options, buildQuery(testPath), createHeaders(testPath));
            assertNotNull(info, "Call " + i + " should succeed");
        }

        // Verify only one connection was created
        assertEquals(1, getCacheSize(), "Only one connection should be cached");
    }

    /**
     * Test that the underlying ArrowFlightConnection is properly stored.
     */
    @Test
    void testConnectionIsStoredWithClient() throws Exception {
        var options = createOptions("PT10M");

        FlightSqlClientPool.INSTANCE.getInfo(options, buildQuery(testPath), createHeaders(testPath));

        ArrowFlightConnection connection = getCachedConnection(options);
        assertNotNull(connection, "ArrowFlightConnection should be stored in cache");
        assertFalse(connection.isClosed(), "Connection should not be closed while in cache");
    }

    /**
     * Test cache key generation is deterministic regardless of map iteration order.
     */
    @Test
    void testCacheKeyIsDeterministic() throws Exception {
        // Create two options with same values
        var options1 = createOptionsWithPath("PT10M", testPath);
        var options2 = createOptionsWithPath("PT10M", testPath);

        FlightSqlClientPool.INSTANCE.getInfo(options1, buildQuery(testPath), createHeaders(testPath));
        FlightSqlClientPool.INSTANCE.getInfo(options2, buildQuery(testPath), createHeaders(testPath));

        // Should reuse the same connection since options are equivalent
        assertEquals(1, getCacheSize(), "Equivalent options should use the same cache key");
    }

    /**
     * Test concurrent access to the pool.
     */
    @Test
    void testConcurrentAccess() throws Exception {
        var options = createOptions("PT10M");
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        FlightInfo[] results = new FlightInfo[threadCount];
        Exception[] exceptions = new Exception[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                try {
                    results[index] = FlightSqlClientPool.INSTANCE.getInfo(options, buildQuery(testPath), createHeaders(testPath));
                } catch (Exception e) {
                    exceptions[index] = e;
                }
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify no exceptions
        for (int i = 0; i < threadCount; i++) {
            assertNull(exceptions[i], "Thread " + i + " should not throw exception: " + exceptions[i]);
            assertNotNull(results[i], "Thread " + i + " should get a result");
        }

        // Verify only one connection was created despite concurrent access
        assertEquals(1, getCacheSize(), "Only one connection should be cached despite concurrent access");
    }

    // Helper methods

    private DatasourceOptions createOptions(String timeout) {
        return DatasourceOptions.parse(Map.of(
                "url", URL,
                "connection_timeout", timeout,
                "username", "admin",
                "password", "admin",
                "path", testPath,
                "function", "read_parquet"
        ));
    }

    private DatasourceOptions createOptionsWithPath(String timeout, String path) {
        return DatasourceOptions.parse(Map.of(
                "url", URL,
                "connection_timeout", timeout,
                "username", "admin",
                "password", "admin",
                "path", path,
                "function", "read_parquet"
        ));
    }

    private String buildQuery(String path) {
        return "SELECT * FROM read_parquet('" + path + "/**/*.parquet')";
    }

    private HeaderCallOption createHeaders(String path) {
        var headers = new FlightCallHeaders();
        headers.insert(Headers.HEADER_PATH, path);
        headers.insert(Headers.HEADER_FUNCTION, "read_parquet");
        return new HeaderCallOption(headers);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getCache() throws Exception {
        var cacheField = FlightSqlClientPool.class.getDeclaredField("cache");
        cacheField.setAccessible(true);
        return (Map<String, Object>) cacheField.get(FlightSqlClientPool.INSTANCE);
    }

    private int getCacheSize() throws Exception {
        return getCache().size();
    }

    private String getCacheKey(DatasourceOptions options) throws Exception {
        var getKeyMethod = FlightSqlClientPool.class.getDeclaredMethod("getKey", DatasourceOptions.class);
        getKeyMethod.setAccessible(true);
        return (String) getKeyMethod.invoke(FlightSqlClientPool.INSTANCE, options);
    }

    private Object getCachedEntry(DatasourceOptions options) throws Exception {
        String key = getCacheKey(options);
        return getCache().get(key);
    }

    private FlightSqlClient getCachedClient(DatasourceOptions options) throws Exception {
        Object entry = getCachedEntry(options);
        if (entry == null) return null;

        var flightClientField = entry.getClass().getDeclaredMethod("flightClient");
        return (FlightSqlClient) flightClientField.invoke(entry);
    }

    private ArrowFlightConnection getCachedConnection(DatasourceOptions options) throws Exception {
        Object entry = getCachedEntry(options);
        if (entry == null) return null;

        var connectionMethod = entry.getClass().getDeclaredMethod("connection");
        return (ArrowFlightConnection) connectionMethod.invoke(entry);
    }

    private long getCachedTimestamp(DatasourceOptions options) throws Exception {
        Object entry = getCachedEntry(options);
        if (entry == null) return -1;

        var timestampMethod = entry.getClass().getDeclaredMethod("timestamp");
        return (long) timestampMethod.invoke(entry);
    }
}
