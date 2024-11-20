import java.sql.*;
import java.util.*;

public class MeshJoin {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/demo";
    private static final String USER = "root";
    private static final String PASS = "12345678";
    private static final int DISK_BUFFER_SIZE = 1000; // Adjust as needed

    private Connection dwConn;
    private Queue<Map<String, Object>> transactionQueue;
    private Map<Integer, Map<String, Object>> hashTable;
    private List<Map<String, Object>> diskBuffer;

    public MeshJoin() throws SQLException {
        dwConn = DriverManager.getConnection(DB_URL, USER, PASS);
        transactionQueue = new LinkedList<>();
        hashTable = new HashMap<>();
        diskBuffer = new ArrayList<>(DISK_BUFFER_SIZE);
    }

    public void executeMeshJoin() throws SQLException {
        while (true) {
            System.out.println("Loading transactions...");
            loadTransactions();
            System.out.println("Loading next partition of CUSTOMERS...");
            loadNextPartition("CUSTOMERS");
            System.out.println("Loading next partition of PRODUCTS...");
            loadNextPartition("PRODUCTS");
            System.out.println("Performing join...");
            performJoin();
            System.out.println("Removing oldest transactions...");
            removeOldestTransactions();
        }
    }

    private void loadTransactions() throws SQLException {
        String query = "SELECT * FROM FACT_TRANSACTIONS LIMIT ?";
        try (PreparedStatement stmt = dwConn.prepareStatement(query)) {
            stmt.setInt(1, DISK_BUFFER_SIZE);
            ResultSet rs = stmt.executeQuery();
            int count = 0;
            while (rs.next()) {
                Map<String, Object> transaction = new HashMap<>();
                transaction.put("ORDER_ID", rs.getInt("ORDER_ID"));
                transaction.put("CUSTOMER_ID", rs.getInt("CUSTOMER_ID"));
                transaction.put("PRODUCT_ID", rs.getInt("PRODUCT_ID"));
                transaction.put("QUANTITY", rs.getInt("QUANTITY"));
                transactionQueue.add(transaction);
                hashTable.put(rs.getInt("ORDER_ID"), transaction);
                count++;
            }
            System.out.println("Loaded " + count + " transactions.");
        } catch (SQLException e) {
            System.err.println("Error loading transactions: " + e.getMessage());
            throw e;
        }
    }

    private void loadNextPartition(String tableName) throws SQLException {
        String query = "SELECT * FROM " + tableName + " LIMIT ?";
        try (PreparedStatement stmt = dwConn.prepareStatement(query)) {
            stmt.setInt(1, DISK_BUFFER_SIZE);
            ResultSet rs = stmt.executeQuery();
            diskBuffer.clear();
            int count = 0;
            while (rs.next()) {
                Map<String, Object> record = new HashMap<>();
                // Add relevant fields from the table
                if (tableName.equals("CUSTOMERS")) {
                    record.put("CUSTOMER_ID", rs.getInt("CUSTOMER_ID"));
                    // ...additional fields...
                } else if (tableName.equals("PRODUCTS")) {
                    record.put("PRODUCT_ID", rs.getInt("PRODUCT_ID"));
                    record.put("PRODUCT_PRICE", rs.getDouble("PRODUCT_PRICE"));
                    record.put("SUPPLIER_NAME", rs.getString("SUPPLIER_NAME"));
                    // ...additional fields...
                }
                diskBuffer.add(record);
                count++;
            }
            System.out.println("Loaded " + count + " records from " + tableName + ".");
        }
    }

    private void performJoin() throws SQLException {
        int joinCount = 0;
        for (Map<String, Object> record : diskBuffer) {
            for (Map<String, Object> transaction : hashTable.values()) {
                if (record.get("CUSTOMER_ID").equals(transaction.get("CUSTOMER_ID")) &&
                    record.get("PRODUCT_ID").equals(transaction.get("PRODUCT_ID"))) {
                    // Enrich transaction with additional data
                    transaction.put("TOTAL_SALE", (int) transaction.get("QUANTITY") * (double) record.get("PRODUCT_PRICE"));
                    // Insert enriched transaction into DW
                    insertIntoDW(transaction, record.get("SUPPLIER_NAME").toString());
                    joinCount++;
                }
            }
        }
        System.out.println("Performed " + joinCount + " joins.");
    }

    private void insertIntoDW(Map<String, Object> transaction, String supplierName) throws SQLException {
        String query = "INSERT INTO FACT_TRANSACTIONS (ORDER_ID, DATE_ID, PRODUCT_ID, CUSTOMER_ID, SUPPLIER_NAME, STORE_ID, QUANTITY, PRODUCT_PRICE, SALE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE QUANTITY = VALUES(QUANTITY), PRODUCT_PRICE = VALUES(PRODUCT_PRICE), SALE = VALUES(SALE)";
        try (PreparedStatement stmt = dwConn.prepareStatement(query)) {
            stmt.setInt(1, (int) transaction.get("ORDER_ID"));
            stmt.setInt(2, (int) transaction.get("DATE_ID"));
            stmt.setInt(3, (int) transaction.get("PRODUCT_ID"));
            stmt.setInt(4, (int) transaction.get("CUSTOMER_ID"));
            stmt.setString(5, supplierName);
            stmt.setInt(6, (int) transaction.get("STORE_ID"));
            stmt.setInt(7, (int) transaction.get("QUANTITY"));
            stmt.setDouble(8, (double) transaction.get("PRODUCT_PRICE"));
            stmt.setDouble(9, (double) transaction.get("TOTAL_SALE"));
            stmt.executeUpdate();
        }
    }

    private void removeOldestTransactions() {
        if (!transactionQueue.isEmpty()) {
            Map<String, Object> oldestTransaction = transactionQueue.poll();
            hashTable.remove(oldestTransaction.get("ORDER_ID"));
            System.out.println("Removed oldest transaction with ORDER_ID: " + oldestTransaction.get("ORDER_ID"));
        }
    }

    public static void main(String[] args) {
        try {
            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter database name: ");
            String dbName = scanner.nextLine();
            
            System.out.print("Enter username: ");
            String username = scanner.nextLine();
            
            System.out.print("Enter password: ");
            String password = scanner.nextLine();
            
            scanner.close();
            
            MeshJoin meshJoin = new MeshJoin();
            
            System.out.println("Starting MESHJOIN process...");
            meshJoin.executeMeshJoin();
            
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
