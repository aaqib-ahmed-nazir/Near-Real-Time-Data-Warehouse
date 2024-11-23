# Designing and Implementing a Near-Real-Time Data Warehouse for METRO Pakistan

## Project Overview
This project implements a near-real-time Data Warehouse (DW) prototype for METRO shopping stores in Pakistan. It integrates transactional data from various sources, enriches it with Master Data (MD) using the MESHJOIN algorithm, and enables advanced business intelligence through OLAP queries. The project is built using Java and SQL and structured as follows:

## Project Structure
The project is structured as follows:

    ├── proprocessing
    │   ├── proprocessing.ipynb # optional to fix the supplierID issues in products_data.csv
    ├── sql
    │   ├── create_master_data.sql
    │   ├── create_dw.sql
    │   ├── olap_queries.sql
    ├── src
    │   ├── load_master_data.java
    │   ├── meshjoin.java
    │   ├── insert_data_into_dw.java
    ├── README.md


## Prerequisites
Before running the project, ensure you have the following installed:
1. **Java Development Kit (JDK)**:  
   Download the latest version of JDK.  
   [Download JDK](https://www.oracle.com/java/technologies/javase-downloads.html)

2. **Eclipse IDE**:  
   Install Eclipse IDE for Java Developers.  
   [Download Eclipse](https://www.eclipse.org/downloads/)

3. **MySQL Server**:  
   Install and set up MySQL server.  
   [Download MySQL](https://dev.mysql.com/downloads/installer/)

4. **MySQL Connector/J**:  
   Add the MySQL JDBC connector to enable database connectivity.  
   [Download MySQL Connector/J](https://dev.mysql.com/downloads/connector/j/)

## Project Setup and Execution

### 1. Clone or Download the Project
Clone the repository or download the ZIP file:
    
```bash
git clone https://github.com/aaqib-ahmed-nazir/AaqibAhmedNazir_22i1920_Project.git
```
    
### 2. Import the Project in Eclipse

1. Open Eclipse IDE.
2. Go to File → Import.
3. Select General → Existing Projects into Workspace.
4. Click Next.
5. Browse to the root directory of the project (where the `src` and `sql` folders are located).
6. Select the project and click Finish to import it.

### 3. Set Up the Database

1. Start your MySQL server.
2. Open MySQL Workbench or any SQL client.
3. Run the following SQL script to create the master data schema:
    - `create_master_data.sql`: Creates tables for products, customers, and stores.
4. Populate the master data by executing:
        - `load_master_data.java`: Loads initial master data into the database.

### 4. Create the Data Warehouse

1. Run the SQL script `create_dw.sql`:
    - This creates the star-schema for the Data Warehouse with fact and dimension tables.

### 5. Execute the MESHJOIN Algorithm

1. In Eclipse, locate and run `meshjoin.java`:
    - This implements the MESHJOIN algorithm to transform and enrich transactional data with master data.

### 6. Insert Data into the Data Warehouse

1. Run `insert_data_into_dw.java`:
    - This inserts the enriched data into the Data Warehouse schema created earlier.

### 7. Execute OLAP Queries

1. Open the SQL script `olap_queries.sql`.
2. Run the queries in the script to perform advanced business intelligence tasks, such as:
    - Analyzing top-performing products.
    - Tracking store revenue trends.
    - Performing seasonal sales analysis.

### Detailed Instructions to Run in Eclipse

1. Add MySQL Connector:
     - Right-click on the project in Eclipse’s Project Explorer.
    - Go to Build Path → Add External JARs.
    - Browse to the MySQL Connector JAR file and add it to the project.
2. Run Java Files:
    - Right-click on the desired `.java` file (e.g., `meshjoin.java`) in Eclipse.
    - Select Run As → Java Application.
    - Input database credentials (if prompted):
        - Host: `localhost`
        - Port: `3306`
        - Username: `<your_mysql_username>`
        - Password: `<your_mysql_password>`
3. Troubleshooting:
    - Ensure MySQL Server is running and that the database credentials in the Java files match your setup.
    - Check the Eclipse console for any errors and debug accordingly.

### Compile and Run the Project

1. Compile the Java files:
    ```bash
    javac -cp .:path/to/mysql-connector-java.jar src/*.java
    ```

2. Run the Java files:
    ```bash
    java -cp .:path/to/mysql-connector-java.jar src.meshjoin
    java -cp .:path/to/mysql-connector-java.jar src.insert_data_into_dw
    ```

## Project Contributors 
[AAQIB AHMED NAZIR](https://github.com/aaqib-ahmed-nazir)

## License
This project is licensed under the MIT License - see the [LICENSE](https://opensource.org/license/MIT) file for details.






