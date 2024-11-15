package src;
import java.io.*;
import java.util.*;

public class MeshJoin 
{
    private static final int CHUNK_SIZE = 1000; // Number of transactions to process at once

    static class Customer 
    {
        int customerId;
        String customerName;
        String gender;

        Customer(int id, String name, String gender) 
        {
            this.customerId = id;
            this.customerName = name;
            this.gender = gender;
        }
    }

    static class Product 
    {
        int productId;
        String productName;
        String productPrice;
        int supplierId;
        String supplierName;
        int storeId;
        String storeName;

        Product(int productId, String productName, String productPrice,
                int supplierId, String supplierName, int storeId, String storeName) 
        {
            this.productId = productId;
            this.productName = productName;
            this.productPrice = productPrice;
            this.supplierId = supplierId;
            this.supplierName = supplierName;
            this.storeId = storeId;
            this.storeName = storeName;
        }
    }

    private static Map<Integer, Customer> loadCustomers(String filePath) {
        Map<Integer, Customer> customers = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(new File(filePath)))) {
            String line = br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                try {
                    String[] data = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                    String customerName = data[1].replaceAll("\"", "");
                    String gender = data[2].replaceAll("\"", "");

                    customers.put(
                            Integer.parseInt(data[0].trim()),
                            new Customer(Integer.parseInt(data[0].trim()), customerName, gender));
                } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                    System.err.println("Error parsing line: " + line + "\nError: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading customer data: " + e.getMessage());
        }
        return customers;
    }

    private static Map<Integer, Product> loadProducts(String filePath) {
        Map<Integer, Product> products = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(new File(filePath)))) {
            String line = br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                try {
                    String[] data = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1); // Split considering quotes
                    // Clean quotes from strings
                    String productName = data[1].replaceAll("\"", "");
                    String productPrice = data[2].replaceAll("\"", "").replaceAll("\\$", "");
                    String supplierName = data[4].replaceAll("\"", "");
                    String storeName = data[6].replaceAll("\"", "");

                    products.put(
                            Integer.parseInt(data[0].trim()),
                            new Product(
                                    Integer.parseInt(data[0].trim()),
                                    productName,
                                    productPrice,
                                    Integer.parseInt(data[3].trim()),
                                    supplierName,
                                    Integer.parseInt(data[5].trim()),
                                    storeName));
                } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                    System.err.println("Error parsing line: " + line + "\nError: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading product data: " + e.getMessage());
        }
        return products;
    }

    private static void processTransactions(String transactionFile,
            Map<Integer, Customer> customers,
            Map<Integer, Product> products,
            String outputFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(transactionFile));
                BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {

            // Updated header with new format
            bw.write("ORDER_ID,ORDER_DATE,PRODUCT_ID,CUSTOMER_ID,CUSTOMER_NAME,GENDER," +
                    "QUANTITY,PRODUCT_NAME,PRODUCT_PRICE,SUPPLIER_ID,SUPPLIER_NAME," +
                    "STORE_ID,STORE_NAME,SALE\n");

            String line = br.readLine(); // Skip header
            List<String> chunk = new ArrayList<>();

            while ((line = br.readLine()) != null) {
                chunk.add(line);

                if (chunk.size() >= CHUNK_SIZE) {
                    processChunk(chunk, customers, products, bw);
                    chunk.clear();
                }
            }

            // Process remaining records
            if (!chunk.isEmpty()) {
                processChunk(chunk, customers, products, bw);
            }

        } catch (IOException e) {
            System.err.println("Error processing transactions: " + e.getMessage());
        }
    }

    private static void processChunk(List<String> chunk,
            Map<Integer, Customer> customers,
            Map<Integer, Product> products,
            BufferedWriter writer) throws IOException {
        for (String transaction : chunk) {
            String[] data = transaction.split(",");
            String orderId = data[0];
            String orderDate = data[1];
            int productId = Integer.parseInt(data[2]);
            double quantity = Double.parseDouble(data[3]);
            int customerId = Integer.parseInt(data[4]);

            Product product = products.get(productId);
            Customer customer = customers.get(customerId);

            if (product != null && customer != null) {
                // Calculate SALE
                double price = Double.parseDouble(product.productPrice);
                double sale = quantity * price;

                // Write in new format with all required fields
                writer.write(String.format("%s,%s,%d,%d,%s,%s,%s,%s,%s,%d,%s,%d,%s,%.2f\n",
                        orderId, 
                        orderDate,
                        productId,
                        customerId, 
                        customer.customerName,
                        customer.gender,
                        String.format("%.0f", quantity),
                        product.productName,
                        product.productPrice,
                        product.supplierId,
                        product.supplierName,
                        product.storeId,
                        product.storeName,
                        sale));
            }
        }
        writer.flush();
    }

    public static void main(String[] args) {
        // Create data directory path
        String dataPath = "src/data/";

        System.out.println("Loading dimension tables...");
        Map<Integer, Customer> customers = loadCustomers(dataPath + "customers_data.csv");
        if (customers.isEmpty()) {
            System.err.println("Failed to load customer data");
            return;
        }

        Map<Integer, Product> products = loadProducts(dataPath + "products_data.csv");
        if (products.isEmpty()) {
            System.err.println("Failed to load product data");
            return;
        }

        System.out.println("Processing transactions...");
        processTransactions(dataPath + "transactions_data.csv",
                          customers, 
                          products,
                          dataPath + "enriched_data.csv");

        System.out.println("MeshJoin completed successfully!");
    }
}
