import java.sql.*;
import java.util.*;
import java.sql.Date;
import java.util.concurrent.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Scanner;

// Constants for MESHJOIN
public class MeshJoin {    
    private static final int BUFFER_SIZE = 1000;
    private static final int NUM_THREADS = 4;

    // Remove static final database configurations
    // private static final String MASTER_DB_URL =
    // "jdbc:mysql://localhost:3306/master_database";
    // private static final String DW_DB_URL = "jdbc:mysql://localhost:3306/dw";
    // private static final String USER = "root";
    // private static final String PASS = "12345678";
    private String masterDbUrl;
    private String dwDbUrl;
    private String user;
    private String pass;

    // Data structures for MESHJOIN
    private List<Map<String, Object>> diskBuffer;
    private Map<Integer, List<Transaction>> hashTable;
    private Queue<Chunk> queue;
    private List<Transaction> streamBuffer;

    // Connection for Data Warehouse
    private Connection dwConnection;
    private PreparedStatement pstmt; // PreparedStatement for batch inserts
    private static final int BATCH_SIZE = 1000;

    private Set<Integer> insertedOrderIds;

    public MeshJoin(String masterDbUrl, String dwDbUrl, String user, String pass) {
        /* 
            parameters: 
                - masterDbUrl - URL of the master database
                - dwDbUrl - URL of the Data Warehouse
                - user - database username
                - pass - database password

            returns: void

            This function initializes the MeshJoin object with the provided database configurations.
        */
        
        this.masterDbUrl = masterDbUrl;
        this.dwDbUrl = dwDbUrl;
        this.user = user;
        this.pass = pass;

        diskBuffer = new ArrayList<>();
        hashTable = new ConcurrentHashMap<>();
        queue = new ConcurrentLinkedQueue<>();
        streamBuffer = new ArrayList<>();
        insertedOrderIds = ConcurrentHashMap.newKeySet();
        try {
            // Establishibng connection to Data Warehouse
            dwConnection = DriverManager.getConnection(dwDbUrl, user, pass);
            dwConnection.setAutoCommit(false);

            // Getting existing orderIds from the Data Warehouse
            String existingIdsQuery = "SELECT ORDER_ID FROM MERGED_DATA";
            try (Statement stmt = dwConnection.createStatement();
                    ResultSet rs = stmt.executeQuery(existingIdsQuery)) {
                while (rs.next()) {
                    insertedOrderIds.add(rs.getInt("ORDER_ID"));
                }
            }

            String sql = "INSERT INTO MERGED_DATA (ORDER_ID, ORDER_DATE, ORDER_TIME, productID, customerID, QUANTITY, TIME_ID, "
                    +
                    "productName, productPrice, supplierID, supplierName, storeID, storeName, " +
                    "customer_name, gender) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            pstmt = dwConnection.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Transaction class for storing transaction data
    static class Transaction {
        int orderId;
        Date orderDate;
        Time orderTime;
        int productId;
        int quantity;
        int customerId;
        int timeId;
    }


    // Chunk class for storing a partition of transactions
    static class Chunk {
        List<Transaction> transactions;
        int partitionsSeen;

        Chunk(List<Transaction> transactions) {
            this.transactions = transactions;
            this.partitionsSeen = 0;
        }
    }

    private List<Map<String, Object>> loadMasterDataPartition(Connection conn, int offset) throws SQLException {

        /*
            parameters: 
                - conn - Connection object to the master database
                - offset - offset for loading the partition

            returns: List<Map<String, Object>> - partition of master data records
         */
        
        List<Map<String, Object>> partition = new ArrayList<>();
        // Load only products table data
        String sql = "SELECT * FROM products LIMIT ? OFFSET ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, BUFFER_SIZE);
            pstmt.setInt(2, offset);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                // Product info only
                row.put("productID", rs.getInt("productID"));
                row.put("productName", rs.getString("productName"));
                row.put("productPrice", rs.getBigDecimal("productPrice"));
                row.put("supplierID", rs.getInt("supplierID"));
                row.put("supplierName", rs.getString("supplierName"));
                row.put("storeID", rs.getInt("storeID"));
                row.put("storeName", rs.getString("storeName"));
                partition.add(row);
            }
        }
        return partition;
    }

    private Map<String, Object> getCustomerData(int customerId, Connection conn) throws SQLException {

        /*
            parameters: 
                - customerId - ID of the customer
                - conn - Connection object to the master database

            returns: Map<String, Object> - customer data for the given customerId
         */

        String sql = "SELECT customer_name, gender FROM customers WHERE customer_id = ?";
        Map<String, Object> customerData = new HashMap<>();

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, customerId);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                customerData.put("customer_name", rs.getString("customer_name"));
                customerData.put("gender", rs.getString("gender"));
            }
        }
        return customerData;
    }

    private void processPartition(List<Map<String, Object>> partition) {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        List<Future<?>> futures = new ArrayList<>();
        int chunkSize = 2000; // Increased chunk size for fewer tasks

        for (int i = 0; i < partition.size(); i += chunkSize) {
            int end = Math.min(i + chunkSize, partition.size());
            List<Map<String, Object>> subPartition = partition.subList(i, end);

            futures.add(executor.submit(() -> {
                int batchCount = 0;
                Set<Integer> localInsertedOrderIds = ConcurrentHashMap.newKeySet(); // Local tracking for duplicates
                try (Connection conn = DriverManager.getConnection(masterDbUrl, user, pass)) {
                    for (Map<String, Object> masterRecord : subPartition) {
                        int productId = (Integer) masterRecord.get("productID");
                        List<Transaction> matches = hashTable.getOrDefault(productId, Collections.emptyList());

                        for (Transaction transaction : matches) {
                            try {
                                synchronized (insertedOrderIds) {
                                    if (localInsertedOrderIds.contains(transaction.orderId)) {
                                        System.out
                                                .println("Duplicate within partition skipped: " + transaction.orderId);
                                        continue;
                                    }
                                    localInsertedOrderIds.add(transaction.orderId);
                                }
                                insertMergedRecordBatch(transaction, masterRecord, conn);
                                if (++batchCount % BATCH_SIZE == 0) {
                                    executeBatch();
                                }
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }));
        }

        for (Future<?> future : futures) 
        {
            try 
            {
                future.get();
            }

            catch (Exception e) 
            {
                e.printStackTrace();
            }
        }
        
        executeBatch();
        executor.shutdown();

        // Clearing the hashTable after processing the partition
        hashTable.clear();
    }

    private String mapGender(String gender) {
        return gender.trim().substring(0, 1).toUpperCase(); 
    }

    private void insertMergedRecordBatch(Transaction transaction, Map<String, Object> masterRecord, Connection conn)
            throws SQLException {
        // Synchronized block to ensure thread-safe access to insertedOrderIds
        synchronized (insertedOrderIds) {
            // Atomically check and add the orderId to prevent duplicates
            if (!insertedOrderIds.add(transaction.orderId)) {
                // Duplicate orderId detected, skip insertion
                // System.out.println("Duplicate orderId detected and skipped: " +
                // transaction.orderId);
                return;
            }
        }

        // Fetch customer data
        Map<String, Object> customerData = getCustomerData(transaction.customerId, conn);
        if (customerData.isEmpty()) {
            System.out.println("Customer data not found for customerId: " + transaction.customerId);
            return;
        }

        // Insert merged record into Data Warehouse
        pstmt.setInt(1, transaction.orderId);
        pstmt.setDate(2, transaction.orderDate);
        pstmt.setTime(3, transaction.orderTime);
        pstmt.setInt(4, (Integer) masterRecord.get("productID"));
        pstmt.setInt(5, transaction.customerId);
        pstmt.setInt(6, transaction.quantity);
        pstmt.setInt(7, transaction.timeId);
        pstmt.setString(8, (String) masterRecord.get("productName"));
        pstmt.setBigDecimal(9, (java.math.BigDecimal) masterRecord.get("productPrice"));
        pstmt.setInt(10, (Integer) masterRecord.get("supplierID"));
        pstmt.setString(11, (String) masterRecord.get("supplierName"));
        pstmt.setInt(12, (Integer) masterRecord.get("storeID"));
        pstmt.setString(13, (String) masterRecord.get("storeName"));
        pstmt.setString(14, (String) customerData.get("customer_name"));
        pstmt.setString(15, mapGender((String) customerData.get("gender")));
        pstmt.addBatch();
    }

    private void executeBatch() {
        
        /*
            parameters: void

            returns: void

            This function executes the batch insert operation on the Data Warehouse.
         */

        try {
            pstmt.executeBatch();
            dwConnection.commit();
            pstmt.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                dwConnection.rollback();
            } catch (SQLException rollbackEx) {
                rollbackEx.printStackTrace();
            }
        }
    }

    public void executeJoin(String transactionsCsvPath) {
       
        /*
            parameters: 
                - transactionsCsvPath - path to the transactions CSV file

            returns: void

            This function executes the MESHJOIN operation by processing the transactions CSV file.
         */

        System.out.println("Starting MESHJOIN operation...");
        try (Connection masterConn = DriverManager.getConnection(masterDbUrl, user, pass)) {
            
            // Get total number of master data records
            int totalRecords = getTotalMasterRecords(masterConn);
            int numPartitions = (int) Math.ceil((double) totalRecords / BUFFER_SIZE);
            System.out.println("Total master records: " + totalRecords);

            // Load master data partition
            System.out.println("Loading master data...");
            List<Map<String, Object>> masterDataPartition = loadMasterDataPartition(masterConn, 0);
            System.out.println("Loaded " + masterDataPartition.size() + " master data records");

            // Process stream data in chunks using BufferedReader
            try (BufferedReader br = new BufferedReader(new FileReader(transactionsCsvPath))) {
                String line;
                int processedLines = 0;
                int skippedLines = 0;
                
                // Skip header row
                br.readLine();

                while ((line = br.readLine()) != null) {
                    try {
                        Transaction transaction = parseTransaction(line);
                        
                        if (transaction != null) { // Only add if parsing was successful
                            streamBuffer.add(transaction);
                            processedLines++;

                            if (streamBuffer.size() >= BUFFER_SIZE) {
                                processStreamBuffer();
                                // Process the current partition against the hash table
                                processPartition(masterDataPartition);
                                // Clear the hashTable after processing to prevent duplicates
                                hashTable.clear();
                            }
                        } else {
                            skippedLines++;
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing line: " + line);
                        e.printStackTrace();
                        skippedLines++;
                    }
                }

                // Process remaining transactions
                if (!streamBuffer.isEmpty()) {
                    processStreamBuffer();
                    processPartition(masterDataPartition);
                    // Clear the hashTable after final processing
                    hashTable.clear();
                }
                System.out.println("Successfully processed all transactions.");
                System.out.println("The Data Warehouse is ready for querying :).");
                if (skippedLines > 0) {
                    System.out.println("Skipped " + skippedLines + " invalid transactions.");
                }
            }

        } catch (Exception e) {
            System.err.println("Error during MESHJOIN operation:");
            e.printStackTrace();
        }
        System.out.println("MESHJOIN operation completed successfully!");
    }

    private int getTotalMasterRecords(Connection conn) throws SQLException {

        /*
            parameters: 
                - conn - Connection object to the master database

            returns: int - total number of records in the master database
         */

        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM products")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private Transaction parseTransaction(String line) {

        /*
            parameters: 
                - line - a line from the transactions CSV file

            returns: Transaction - a Transaction object parsed from the input line
         */

        try {
            Transaction t = new Transaction();
            String[] parts = line.split(",");
            if (parts.length < 6) {
                throw new IllegalArgumentException("Invalid CSV format: insufficient columns");
            }
            t.orderId = Integer.parseInt(parts[0].trim());

            // Validating and parse ORDER_DATE and ORDER_TIME
            String orderDateTimeStr = parts[1].trim();
            try {
                // Define expected datetime format
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                sdf.setLenient(false);
                java.util.Date parsedDate = sdf.parse(orderDateTimeStr);
                t.orderDate = new java.sql.Date(parsedDate.getTime());
                t.orderTime = new java.sql.Time(parsedDate.getTime());
            } catch (ParseException e) {
                System.err.println(
                        "Invalid ORDER_DATE format for order ID: " + t.orderId + " | Value: " + parts[1].trim());
                return null;
            }

            t.productId = Integer.parseInt(parts[2].trim());
            t.quantity = Integer.parseInt(parts[3].trim());
            t.customerId = Integer.parseInt(parts[4].trim());
            t.timeId = Integer.parseInt(parts[5].trim());
            return t;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Error parsing transaction: " + line, e);
        }
    }

    private void processStreamBuffer() {

        /*
            parameters: void

            returns: void

            This function processes the stream buffer by adding the transactions to the queue and hashTable.
         */

        Chunk chunk = new Chunk(new ArrayList<>(streamBuffer));
        queue.add(chunk);

        for (Transaction t : streamBuffer) {
            hashTable.computeIfAbsent(t.productId, k -> new ArrayList<>()).add(t);
        }

        streamBuffer.clear();
        System.out.println("Successfully processed buffer of " + BUFFER_SIZE + " transactions");
    }

    public void close() {
        
        /*
            parameters: void

            returns: void

            This function closes the PreparedStatement and Connection objects.
         */

        try {
            if (pstmt != null)
                pstmt.close();
            if (dwConnection != null)
                dwConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        /*
            parameters: void

            returns: void

            This function initializes the MESHJOIN operation by taking user input for database configurations.
         */

        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter master database URL (e.g., jdbc:mysql://localhost:3306/master_database): ");
        String masterDbUrl = scanner.nextLine();

        System.out.print("Enter Data Warehouse URL (e.g., jdbc:mysql://localhost:3306/dw): ");
        String dwDbUrl = scanner.nextLine();

        System.out.print("Enter database username: ");
        String user = scanner.nextLine();

        System.out.print("Enter database password: ");
        String pass = scanner.nextLine();

        System.out.println("Initializing MESHJOIN...");
        MeshJoin meshJoin = new MeshJoin(masterDbUrl, dwDbUrl, user, pass);
        String csvPath = "/Users/aaqibnazir/Documents/uni/DWH/AaqibAhmedNazir_22i1920_Project/src/data/transactions.csv";
        System.out.println("Processing transactions from: " + csvPath);
        meshJoin.executeJoin(csvPath);
        meshJoin.close();
    }
}