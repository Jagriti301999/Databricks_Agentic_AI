# Databricks notebook source
spark.sql(f"""
-- Create a function to retrieve company policy details
CREATE OR REPLACE FUNCTION agentic_catalog.agentic_schema.get_return_policy(
    policy_name STRING COMMENT 'Policy name to return. Example policies: Account Cancellation Policy, Exchange Policy, Refund Policy, Warranty Policy, Privacy Policy, Return Policy'
    )
    RETURNS TABLE (
    policy           STRING,
    policy_details   STRING,
    last_updated     DATE
    )
    COMMENT 'Returns the details of the Return Policy'
    LANGUAGE SQL
    RETURN (
    SELECT
    policy,
    policy_details,
    last_updated
    FROM agentic_catalog.agentic_schema.policies
    WHERE policy = policy_name
    LIMIT 1
    );
    """)
####################################################################################

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from agentic_catalog.agentic_schema.policies

# COMMAND ----------

# MAGIC %md
# MAGIC |policy|policy_details|last_updated|
# MAGIC |---|---|---|
# MAGIC |Account Cancellation Policy|Customers have the right to cancel their accounts at any given time. Upon successful cancellation, a confirmation email will be sent to the registered email address. The account data will be retained for a period of 30 days post-cancellation for any potential follow-up actions.|2023-02-20|
# MAGIC |Exchange Policy|Customers can exchange products for a similar item within 30 days of the original purchase date. The exchanged items must be in their original condition and packaging. Note that exchanges do not count towards the annual return limit of 12 items per customer.|2023-01-30|
# MAGIC |Refund Policy|Refunds will be processed within 5-7 business days after the returned item has been received and inspected by our team. The refund will be issued to the original payment method used during the purchase. Customers will be notified via email once the refund has been processed.|2023-03-05|
# MAGIC |Warranty Policy|All products are covered by a one-year warranty that protects against manufacturing defects. Customers can file a warranty claim within this period. The claimed items will be subject to inspection to determine eligibility for repair or replacement.|2023-02-10|
# MAGIC |Privacy Policy|Customer data is safeguarded and will not be shared with third parties without explicit consent from the customer. Customers have the right to request the deletion of their data at any time. Data deletion requests will be processed within 30 days.|2023-01-25|
# MAGIC |Return Policy|Customers are entitled to return 12 items per calendar year and each must be within a period of 30 days from the date of purchase.|2023-01-15|

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from agentic_catalog.agentic_schema.get_return_policy("Return Policy")

# COMMAND ----------

# MAGIC %md
# MAGIC |policy|policy_details|last_updated|
# MAGIC |---|---|---|
# MAGIC |Return Policy|Customers are entitled to return 12 items per calendar year and each must be within a period of 30 days from the date of purchase.|2023-01-15|

# COMMAND ----------

####################################################################################
# Create two functions we will use later in the course
spark.sql(f"""
-- Create a function to get service history by user
CREATE OR REPLACE FUNCTION agentic_catalog.agentic_schema.get_service_history(
    user_email STRING COMMENT 'User email to retrieve order history'
    )
    RETURNS TABLE (
    returns_last_12_months INT,
    issue_category STRING, 
    todays_date DATE
    )
    COMMENT 'This takes the user_name of a customer as an input and returns the number of returns and the issue category'
    LANGUAGE SQL
    RETURN(
    SELECT count(*) as returns_last_12_months, issue_category, now() as todays_date
    FROM agentic_catalog.agentic_schema.cust_service_data 
    WHERE email = user_email
    GROUP BY issue_category
    );""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from agentic_catalog.agentic_schema.cust_service_data limit 1

# COMMAND ----------

# MAGIC %md
# MAGIC |customer_id|name|email|phone_number|address|interaction_id|date_time|issue_category|issue_description|agent_id|
# MAGIC |---|---|---|---|---|---|---|---|---|---|
# MAGIC |453e50e0-232e-44ea-9fe3-28d550be6294|Nicolas Pelaez|nicolas.pelaez@example.com|2327115355|219 Kevin Shores, New Melissafort, PW 78001|2b8f5009-c1ef-49ec-bc06-c2b3e170cfb3|2025-07-11T13:43:48.656+00:00|Returns|Hi, I've been enjoying my new SoundWave X5 Pro Headphones, but suddenly they're not connecting to my phone anymore. They don't show up in my Bluetooth devices list at all. It's frustrating because they were working perfectly yesterday. Is there a way to fix this, or should I proceed with a return?|38|

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from agentic_catalog.agentic_schema.get_service_history("nicolas.pelaez@example.com")

# COMMAND ----------

# MAGIC %md
# MAGIC |returns_last_12_months|issue_category|todays_date|
# MAGIC |---|---|---|
# MAGIC |23|Returns|2026-06-24|
