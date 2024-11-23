import java.sql.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class load_data {
    private static String[] parseCsvLine(String line) {
        /* 
            parameters: line - the line to be parsed
            
            returns: an array of strings containing the parsed values
            
            This function is used to parse a CSV line that may contain commas within
        */

        List<String> result = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder currentStr = new StringBuilder();

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                inQuotes = !inQuotes;
            }

            else if (ch == ',' && !inQuotes) {
                result.add(currentStr.toString().trim().replace("\"", ""));
                currentStr = new StringBuilder();
            }

            else {
                currentStr.append(ch);
            }
        }
        result.add(currentStr.toString().trim().replace("\"", ""));

        return result.toArray(new String[0]);
    }

    public static void main(String[] args) {

        /* 
            parameters: args - command line arguments
            
            returns: void
            
            This function is used to load data from CSV files into the database
        */

        String jdbcURL = "jdbc:mysql://localhost:3306/master_database";
        String username = "root";
        String password = "12345678";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(jdbcURL, username, password);
            System.out.println("Connected to the database. :)");

            // Clearing the existing data
            Statement stmt = connection.createStatement();
            stmt.execute("TRUNCATE TABLE customers");
            stmt.execute("TRUNCATE TABLE products");

            // Loading customers data into the database
            String customersFile = "/Users/aaqibnazir/Documents/uni/DWH/AaqibAhmedNazir_22i1920_Project/src/data/customers_data.csv";
            BufferedReader customersReader = new BufferedReader(new FileReader(customersFile));
            String line;
            customersReader.readLine(); // Skip header
            while ((line = customersReader.readLine()) != null) {
                String[] nextLine = parseCsvLine(line); // Parsing the CSV line
                int customerId = Integer.parseInt(nextLine[0].trim());
                String customerName = nextLine[1].trim();
                String gender = nextLine[2].trim();
                String sql = "REPLACE INTO customers (customer_id, customer_name, gender) VALUES (?, ?, ?)";
                PreparedStatement statement = connection.prepareStatement(sql);
                statement.setInt(1, customerId);
                statement.setString(2, customerName);
                statement.setString(3, gender);
                statement.executeUpdate();
            }
            customersReader.close();
            System.out.println("Customers data has been inserted.");

            // Loading products data into the database
            String productsFile = "/Users/aaqibnazir/Documents/uni/DWH/AaqibAhmedNazir_22i1920_Project/src/data/products_data.csv";
            BufferedReader productsReader = new BufferedReader(new FileReader(productsFile));
            productsReader.readLine(); // Skip header
            while ((line = productsReader.readLine()) != null) {
                String[] nextLine = parseCsvLine(line);
                int productID = Integer.parseInt(nextLine[0].trim());
                String productName = nextLine[1].trim();
                double productPrice = Double.parseDouble(nextLine[2].replace("$", "").trim());
                int supplierID = Integer.parseInt(nextLine[3].trim());
                String supplierName = nextLine[4].trim();
                int storeID = Integer.parseInt(nextLine[5].trim());
                String storeName = nextLine[6].trim();
                String sql = "REPLACE INTO products (productID, productName, productPrice, supplierID, supplierName, storeID, storeName) VALUES (?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement statement = connection.prepareStatement(sql);
                statement.setInt(1, productID);
                statement.setString(2, productName);
                statement.setDouble(3, productPrice);
                statement.setInt(4, supplierID);
                statement.setString(5, supplierName);
                statement.setInt(6, storeID);
                statement.setString(7, storeName);
                statement.executeUpdate();
            }
            productsReader.close();
            System.out.println("Products data has been inserted.");

            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}