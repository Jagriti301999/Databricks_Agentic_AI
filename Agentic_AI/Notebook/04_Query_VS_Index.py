# Databricks notebook source
# DBTITLE 1,Install required packages
# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

import os
from databricks.vector_search.client import VectorSearchClient

workspace_url = os.environ.get("WORKSPACE_URL")
sp_client_id = os.environ.get("SP_CLIENT_ID")
sp_client_secret = os.environ.get("SP_CLIENT_SECRET")

vsc = VectorSearchClient(
    workspace_url=workspace_url,
    service_principal_client_id=sp_client_id,
    service_principal_client_secret=sp_client_secret
)

index = vsc.get_index(endpoint_name="vs_endpoint_1", index_name="agentic_catalog.agentic_schema.product_docs_index")

index.similarity_search(num_results=3, columns=["indexed_doc","product_id"], query_text="where can i find tutorial for AccountEase Pro  product in short ", query_type="HYBRID")

# COMMAND ----------

# MAGIC %md
# MAGIC [
# MAGIC [NOTICE] Using a notebook authentication token. Recommended for development only. For improved performance, please use Service Principal based authentication. To disable this message, pass disable_notice=True.
# MAGIC {'manifest': {'column_count': 3,
# MAGIC   'columns': [{'name': 'indexed_doc'},
# MAGIC    {'name': 'product_id'},
# MAGIC    {'name': 'score'}]},
# MAGIC  'result': {'row_count': 3,
# MAGIC   'data_array': [["<product_category>Software</product_category>\n<product_sub_category>Online Platform</product_sub_category>\n<product_name>AccountEase Pro</product_name>\n<product_doc>\nAccountEase Pro Documentation\nProduct Overview\nAccountEase Pro is a robust online platform designed to simplify\npersonal account management for individuals and businesses alike.\nWith an intuitive interface, users can seamlessly manage their\naccounts, reset passwords, and maintain high security standards.\nGetting Started\nSign Up\nGo to the AccountEase Pro website.\nClick on the 'Sign Up' button.\nFill out the registration form with your name, email, and\npassword.\nConfirm your registration via the verification email.\nLogin\nAccess the portal through the homepage.\nEnter your registered email and password.\nClick 'Login'.\nNavigation\nThe dashboard features user-friendly tabs for quick access to\nyour profile, settings, and support.\nAccount Management\nResetting Your Password\nClick on 'Forgot Password?' on the login page.\nEnter your email address and submit the form.\nCheck your email for a reset link and follow the instructions.\nEnsure the link is not expired (valid for 24 hours).\nUpdating Profile Information1. \n2. \n3. \n4. \n5. \n6. \n7. \n8. \n9. \n10. \n11. \n1. \n2. \n3. \n4. \n5. \n6. \nNavigate to 'My Profile'.\nEdit fields such as your name, email, and phone number.\nSave changes.\nManaging Security Settings\nGo to 'Security Settings' in your account menu.\nEnable two-factor authentication for enhanced security.\nReview login history and change your password regularly.\nCommon Troubleshooting\nProblem: Cannot Reset Password\nEnsure the reset email hasn't gone to the spam folder.

# COMMAND ----------

# MAGIC %md
# MAGIC
