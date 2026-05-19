import java.sql.*;

/**
 * Minimal JDBC smoke test for AnalyticsDB PostgreSQL wire protocol.
 *
 * Usage: java -cp /path/to/postgresql.jar:. JdbcSmokeTest <host> <port> <database> <user> <password>
 *
 * Exits 0 on success, 1 on any failure.
 */
public class JdbcSmokeTest {

    private static void check(boolean condition, String message) {
        if (!condition) {
            System.out.println("JDBC FAIL: " + message);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: JdbcSmokeTest <host> <port> <database> <user> <password>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String database = args[2];
        String user = args[3];
        String password = args[4];

        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("JDBC FAIL: PostgreSQL driver not found: " + e.getMessage());
            System.exit(1);
        }

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            check(conn != null, "connection should not be null");
            check(!conn.isClosed(), "connection should be open");

            // Test 1: SELECT 1
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1 AS result")) {
                check(rs.next(), "SELECT 1 should return a row");
                int val = rs.getInt("result");
                check(val == 1, "SELECT 1 should return 1, got: " + val);
                check(!rs.next(), "SELECT 1 should return exactly one row");
            }

            // Test 2: SELECT version()
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT version()")) {
                check(rs.next(), "SELECT version() should return a row");
                String version = rs.getString(1);
                check(version != null && !version.isEmpty(), "version() should return a non-null, non-empty string");
            }

            // Test 3: CREATE TABLE
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE jdbc_test (id BIGINT NOT NULL, name TEXT)");
            }

            // Test 4: INSERT VALUES
            try (Statement stmt = conn.createStatement()) {
                int affected = stmt.executeUpdate(
                        "INSERT INTO jdbc_test VALUES (1, 'hello'), (2, 'world')");
                check(affected == 2, "INSERT should affect 2 rows, got: " + affected);
            }

            // Test 5: SELECT rows and verify values
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT id, name FROM jdbc_test ORDER BY id")) {
                check(rs.next(), "first row should exist");
                check(rs.getLong("id") == 1L, "first id should be 1, got: " + rs.getLong("id"));
                check("hello".equals(rs.getString("name")),
                        "first name should be 'hello', got: " + rs.getString("name"));

                check(rs.next(), "second row should exist");
                check(rs.getLong("id") == 2L, "second id should be 2, got: " + rs.getLong("id"));
                check("world".equals(rs.getString("name")),
                        "second name should be 'world', got: " + rs.getString("name"));

                check(!rs.next(), "should be exactly 2 rows");
            }

            // Test 6: Parameterized query
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, name FROM jdbc_test WHERE id = ?")) {
                ps.setLong(1, 1L);
                try (ResultSet rs = ps.executeQuery()) {
                    check(rs.next(), "parameterized query should return a row for id=1");
                    check(rs.getLong("id") == 1L,
                            "parameterized result id should be 1, got: " + rs.getLong("id"));
                    check("hello".equals(rs.getString("name")),
                            "parameterized result name should be 'hello', got: " + rs.getString("name"));
                    check(!rs.next(), "parameterized query should return exactly 1 row");
                }
            }

            // Test 7: DatabaseMetaData.getTables (optional — REGCLASS may not be supported)
            try {
                DatabaseMetaData meta = conn.getMetaData();
                try (ResultSet rs = meta.getTables(null, null, "%", null)) {
                    while (rs.next()) {} // drain result set
                }
            } catch (SQLException e) {
                // getTables uses REGCLASS internally which may not be supported; treat as optional
                System.out.println("Note: getTables not fully supported: " + e.getMessage());
            }

            // Test 8: DROP TABLE
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE jdbc_test");
            }

        } catch (SQLException e) {
            System.out.println("JDBC FAIL: SQL exception: " + e.getMessage());
            e.printStackTrace(System.out);
            System.exit(1);
        }

        System.out.println("JDBC SMOKE TEST: PASSED");
        System.exit(0);
    }
}
