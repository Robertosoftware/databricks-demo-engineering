# Define rules for data quality checks

# rules_buyers
rules_buyers = {
    "valid_id":"id IS NOT NULL",
    "valid_name": "name IS NOT NULL",
    "valid_username": "username IS NOT NULL",
    "valid_email":"email IS NOT NULL AND email LIKE '%@%'"
}
quarantine_rules_buyers = "NOT({0})".format(" AND ".join(rules_buyers.values()))


# rules_transactions
rules_transactions = {
    "valid_transaction":"transaction IS NOT NULL",
    "valid_transactioner":"transactioner IS NOT NULL"
}
quarantine_rules_transactions = "NOT({0})".format(" AND ".join(rules_transactions.values()))

# rules_sales
rules_sales = {
    "valid_salesid":"salesid IS NOT NULL", 
    "valid_sales_type":"sales_type IS NOT NULL"
}