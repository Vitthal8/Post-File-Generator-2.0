# India Post Customer File Merger

This Python application automates the merging of multiple customer connection files—each with varying column headers—into a standardized format required by **India Post** for bulk booking or dispatch.

## 🚀 Features

- ✅ Automatically detects and maps varying header names (e.g., `name`, `customer_name`) to a standard format
- ✅ Merges multiple Excel or CSV files into one consolidated file
- ✅ Outputs clean, India Post–ready data for bulk upload
- ✅ Reduces manual formatting errors and saves time

## 🧠 Use Case

Many customers provide connection data in different formats. This tool helps unify those inconsistent headers (e.g., `name`, `customer_name`, `full_name`) into a common structure required by India Post for dispatch processing.

## 📁 Input Requirements

- Multiple `.xlsx` or `.csv` files with customer data
- Files may use different headers like:
  - `name`, `customer_name`, `cust_name`
  - `address`, `full_address`, `addr`
  - `pincode`, `pin`, `postal_code`

## 📤 Output

A single `.xlsx` file with standardized headers such as:

| Name | Address | Pincode | Mobile |
|------|---------|---------|--------|
| John Doe | 123 Main Street, Mumbai | 400001 | 9876543210 |

## 🛠️ Dependencies

```bash
pip install pandas openpyxl
