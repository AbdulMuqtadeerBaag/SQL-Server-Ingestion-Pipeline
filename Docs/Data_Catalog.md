# Data Catalog for Final Layer

## Overview
The Final Layer is the business-level data representation, structured to support analytical and reporting use cases. It consists of **dimension tables** and **fact tables** for specific business metrics.

---

### 1. **final.dim_customers**
- **Purpose:** Stores customer details enriched with demographic and geographic data.
- **Columns:**

| Column Name      | Data Type     | Description                                                                                   |
|------------------|---------------|-----------------------------------------------------------------------------------------------|
| Customer_Key     | INT           | Surrogate key uniquely identifying each customer record in the dimension table.               |
| Customer_Id      | INT           | Unique numerical identifier assigned to each customer.                                        |
| Customer_Number  | VARCHAR(30)   | Alphanumeric identifier representing the customer, used for tracking and referencing.         |
| First_Name       | VARCHAR(50)   | The customer's first name, as recorded in the system.                                         |
| Last_Name        | VARCHAR(50)   | The customer's last name or family name.                                                      |
| Country          | VARCHAR(20)   | The country of residence for the customer (e.g., 'Australia').                                |
| Marital_Status   | VARCHAR(20)   | The marital status of the customer (e.g., 'Married', 'Single').                               |
| Gender           | VARCHAR(15)   | The gender of the customer (e.g., 'Male', 'Female', 'n/a').                                   |
| Birth_Date       | DATE          | The date of birth of the customer, formatted as YYYY-MM-DD (e.g., 1971-10-06).                |
| Create_Date      | DATE          | The date and time when the customer record was created in the system.                         |

---

### 2. **final.dim_products**
- **Purpose:** Provides information about the products and their attributes.
- **Columns:**

| Column Name         | Data Type     | Description                                                                                          |
|---------------------|---------------|------------------------------------------------------------------------------------------------------|
| Product_Key         | INT           | Surrogate key uniquely identifying each product record in the product dimension table.               |
| Product_Id          | INT           | A unique identifier assigned to the product for internal tracking and referencing.                   |
| Product_Number      | VARCHAR(30)   | A structured alphanumeric code representing the product, often used for categorization or inventory. |
| Product_Name        | VARCHAR(50)   | Descriptive name of the product, including key details such as type, color, and size.                |
| Category_Id         | VARCHAR(20)   | A unique identifier for the product's category, linking to its high-level classification.            |
| Category            | VARCHAR(30)   | The broader classification of the product (e.g., Bikes, Components) to group related items.          |
| Sub_Category        | VARCHAR(30)   | A more detailed classification of the product within the category, such as product type.             |
| Maintenance         | VARCHAR(10)   | Indicates whether the product requires maintenance (e.g., 'Yes', 'No').                              |
| Cost                | INT           | The cost or base price of the product, measured in monetary units.                                   |
| Product_Line        | VARCHAR(20)   | The specific product line or series to which the product belongs (e.g., Road, Mountain).             |
| Start_Date          | DATE          | The date when the product became available for sale or use, stored in.                               |

---

### 3. **final.fact_sales**
- **Purpose:** Stores transactional sales data for analytical purposes.
- **Columns:**

| Column Name     | Data Type     | Description                                                                                   |
|-----------------|---------------|-----------------------------------------------------------------------------------------------|
| Order_Number    | VARCHAR(20)   | A unique alphanumeric identifier for each sales order (e.g., 'SO54496').                      |
| Product_Key     | INT           | Surrogate key linking the order to the product dimension table.                               |
| Customer_Key    | INT           | Surrogate key linking the order to the customer dimension table.                              |
| Order_Date      | DATE          | The date when the order was placed.                                                           |
| Shipping_Date   | DATE          | The date when the order was shipped to the customer.                                          |
| Due_Date        | DATE          | The date when the order payment was due.                                                      |
| Sales_Amount    | INT           | The total monetary value of the sale for the line item, in whole currency units (e.g., 25).   |
| Quantity        | INT           | The number of units of the product ordered for the line item (e.g., 1).                       |
| Price           | INT           | The price per unit of the product for the line item, in whole currency units (e.g., 25).      |
