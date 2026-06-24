# Databricks notebook source
# DBTITLE 1,Introduction
# MAGIC %md
# MAGIC # Product Documentation Indexing Pipeline
# MAGIC
# MAGIC This notebook demonstrates how to:
# MAGIC 1. Load product and documentation data from Unity Catalog tables
# MAGIC 2. Join the tables based on product name
# MAGIC 3. Create an indexed document format for downstream processing
# MAGIC 4. Save the results to a new table
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `agentic_catalog.agentic_schema.products` - Product metadata
# MAGIC - `agentic_catalog.agentic_schema.product_docs` - Product documentation
# MAGIC
# MAGIC **Target Table:**
# MAGIC - `agentic_catalog.agentic_schema.product_docs_combined` - Joined data with indexed documentation

# COMMAND ----------

# DBTITLE 1,Step 1: Import Required Libraries
# MAGIC %md
# MAGIC ## Step 1: Import Required Libraries
# MAGIC
# MAGIC We'll use PySpark for distributed data processing and table operations.

# COMMAND ----------

# DBTITLE 1,Import libraries
# Import PySpark SQL functions
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

print("✓ Libraries imported successfully")

# COMMAND ----------

# DBTITLE 1,Step 2: Load Source Tables
# MAGIC %md
# MAGIC ## Step 2: Load Source Tables
# MAGIC
# MAGIC Load both source tables from Unity Catalog and examine their schemas.

# COMMAND ----------

# DBTITLE 1,Load products table
# Load the products table
products_df = spark.table("agentic_catalog.agentic_schema.products")

print("Products Table Schema:")
products_df.printSchema()
print(f"\nTotal products: {products_df.count()}")

# Display sample data
print("\nSample products:")
display(products_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC |product_id|product_name|product_category|product_sub_category|
# MAGIC |---|---|---|---|
# MAGIC |9eee3547-9ca0-4fe4-8be5-a6c2dd2ef96d|BrownBox SwiftWatch X500|Gadgets|Smartwatch|
# MAGIC |bff42cfb-2730-490c-9da8-a0987e686ec9|SmartX Pro|Wearable Technology|Smartwatch|
# MAGIC |3de8f0d9-7e14-4673-b181-61194ababee4|StridePro Runner|Men/Women/Kids|Shoes|
# MAGIC |594ef1d3-3dfc-4829-9e98-5276c70c152f|Urban Explorer Jacket|Men/Women/Kids|Clothing|
# MAGIC |b739e42e-ba7d-40da-8e4b-f5472506a442|Elegance Extendable Dining Table|Furniture|Dining Table|

# COMMAND ----------

# DBTITLE 1,Load product_docs table
# Load the product documentation table
product_docs_df = spark.table("agentic_catalog.agentic_schema.product_docs")

print("Product Docs Table Schema:")
product_docs_df.printSchema()
print(f"\nTotal documents: {product_docs_df.count()}")

# Display sample data (truncate long text)
print("\nSample documents:")
display(product_docs_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Product Docs Table Schema:
# MAGIC root
# MAGIC  |-- product_name: string (nullable = true)
# MAGIC  |-- product_doc: string (nullable = true)
# MAGIC
# MAGIC
# MAGIC Total documents: 509
# MAGIC
# MAGIC Sample documents:
# MAGIC |product_name|product_doc|
# MAGIC |---|---|
# MAGIC |AccountEase Pro|AccountEase Pro Documentation<br>Product Overview<br>AccountEase Pro is a robust online platform designed to simplify<br>personal account management for individuals and businesses alike.<br>With an intuitive interface, users can seamlessly manage their<br>accounts, reset passwords, and maintain high security standards.<br>Getting Started<br>Sign Up<br>Go to the AccountEase Pro website.<br>Click on the 'Sign Up' button.<br>Fill out the registration form with your name, email, and<br>password.<br>Confirm your registration via the verification email.<br>Login<br>Access the portal through the homepage.<br>Enter your registered email and password.<br>Click 'Login'.<br>Navigation<br>The dashboard features user-friendly tabs for quick access to<br>your profile, settings, and support.<br>Account Management<br>Resetting Your Password<br>Click on 'Forgot Password?' on the login page.<br>Enter your email address and submit the form.<br>Check your email for a reset link and follow the instructions.<br>Ensure the link is not expired (valid for 24 hours).<br>Updating Profile Information1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>9. <br>10. <br>11. <br>1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>Navigate to 'My Profile'.<br>Edit fields such as your name, email, and phone number.<br>Save changes.<br>Managing Security Settings<br>Go to 'Security Settings' in your account menu.<br>Enable two-factor authentication for enhanced security.<br>Review login history and change your password regularly.<br>Common Troubleshooting<br>Problem: Cannot Reset Password<br>Ensure the reset email hasn't gone to the spam folder.<br>Verify the email you entered is correct and registered.<br>If the link is not working, request a new reset link.<br>Problem: Email Verification Issues<br>Check for a verification email and follow the link inside.<br>Resend the verification email if not received within a few<br>minutes.<br>Problem: Account Locked<br>Accounts may lock after consecutive failed login attempts.<br>Contact support to unlock your account.<br>Advanced Features<br>Custom Integrations<br>Connect AccountEase Pro with third-party applications like<br>Google Drive or Dropbox.<br>Use API keys for seamless data flow.<br>User Customization<br>Personalize the dashboard theme and notification settings.7. <br>8. <br>9. <br>10. <br>11. <br>12. <br>13. <br>1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>9. <br>10. <br>1. <br>2. <br>3. <br>4. <br>5. <br>Support and Resources<br>FAQs<br>Visit our FAQ section for quick answers.<br>Customer Support<br>Reach out to support via live chat or email at<br>support@accounteasepro.com.<br>Useful Links<br>Tutorial Videos: www.accounteasepro.com/tutorials<br>User Forum: www.accounteasepro.com/forum<br>For further assistance, refer to our detailed support guides and video<br>tutorials available through the platform. Welcome to AccountEase Pro,<br>where managing your account is made easy and secure.• <br>• <br>• <br>• <br>• <br>• <br>•|
# MAGIC |AccuBooks Pro|AccuBooks Pro: Comprehensive User<br>Documentation<br>Table of Contents<br>Introduction<br>System Requirements<br>Installation Guide<br>Core Features<br>Getting Started<br>Troubleshooting<br>Frequently Asked Questions<br>T echnical Support<br>Updates and Maintenance<br>Compliance and Security<br>1. Introduction<br>AccuBooks Pro is a cutting-edge accounting software designed for<br>small to medium-sized businesses. With a user-friendly interface and<br>robust functionality, it streamlines financial management tasks such<br>as invoicing, ledger maintenance, tax compliance, payroll, and<br>customized reporting.<br>2. System Requirements<br>Operating System : Windows 10 or later, macOS 10.14 or later<br>Processor : 1 GHz or faster<br>Memory : 8 GB RAM<br>Storage : 500 MB of available hard disk space<br>Internet : Broadband connection for updates and cloud features<br>3. Installation Guide<br>Step 1:  Download the AccuBooks Pro installer from our official<br>website.1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>9. <br>10. <br>• <br>• <br>• <br>• <br>• <br>Step 2:  Double-click the downloaded file to begin the installation.<br>Step 3:  Follow the on-screen instructions to complete the setup.<br>Step 4:  Enter your license key when prompted to activate the<br>software.<br>Common Installation Issues:  - Error 404 : Re-download the<br>installer as this error indicates a corrupted file. - Error 302 : Ensure<br>your operating system is up-to-date before re-attempting installation.<br>4. Core Features<br>Transaction Management : Record and categorize financial<br>transactions with ease.<br>Invoicing : Create and send professional invoices directly from<br>the software.<br>Reporting : Generate detailed financial reports for better<br>analysis and strategic planning.<br>Payroll Management : Simplify payroll processing with<br>automatic tax calculations and payslip generation.<br>5. Getting Started<br>Creating a New Account:  - Navigate to the Accounts module. -<br>Click 'New Account' and follow the guided setup process.<br>Entering Transactions:  - Access the Transactions module. - Select<br>'New Transaction' and input the relevant details.<br>Generating Reports:  - Go to the Reports section. - Choose the type<br>of report and customize the parameters as needed.<br>6. Troubleshooting<br>Software Crashes During Report Generation:  Use the web<br>version as an alternative; ensure you have the latest software<br>update.<br>Installation Issues:  Verify system compatibility and consult the<br>installation issues section for specific error codes.• <br>• <br>• <br>• <br>7. Frequently Asked Questions<br>Q: How do I recover lost data?  A: Navigate to the Backup &<br>Restore section under Settings to explore recovery options.<br>Q: Can I integrate external applications?  A: Yes, AccuBooks<br>Pro supports various third-party integrations. Visit our website<br>for a list of available integrations.<br>8. Technical Support<br>For additional support, please contact our technical team: - Email:<br>support@accubookspro.com - Phone:  1-800-555-0199<br>9. Updates and Maintenance<br>Regularly check for software updates under the Help menu to ensure<br>optimal performance. It is advised to perform backups before major<br>updates.<br>10. Compliance and Security<br>AccuBooks Pro ensures compliance with regional tax regulations and<br>incorporates robust security features such as data encryption, multi-<br>factor authentication, and audit trails.<br>For further details, refer to the in-app Help section or visit our online<br>knowledge base.• <br>•|
# MAGIC |AcoustiWave AirBuds Pro|AcoustiWave AirBuds Pro Documentation<br>Product Overview<br>The AcoustiWave AirBuds Pro are premium wireless earbuds designed<br>for superior sound and seamless connectivity. Featuring advanced<br>Bluetooth 5.2 technology, these earbuds provide a high-fidelity audio<br>experience with minimal latency, making them perfect for music<br>lovers and on-the-go professionals.<br>Key Features<br>Bluetooth 5.2 Connectivity : Ensures a stable and fast<br>connection with all Bluetooth-enabled devices.<br>Voice Assistant Support : Compatible with Siri and Google<br>Assistant for hands-free control.<br>Noise Cancellation : Active Noise Cancellation (ANC) for an<br>immersive listening experience.<br>Battery Life : Provides up to 7 hours of playtime on a single<br>charge, with an additional 21 hours from the charging case.<br>Ergonomic Design : Lightweight and comfortable fit for<br>extended wear.<br>Getting Started<br>Charging : Fully charge the earbuds and the case using the<br>included USB-C cable.<br>Power On : Remove the earbuds from the case; they will<br>automatically power on.<br>Pairing :<br>Turn on Bluetooth on your device.<br>Select 'AcoustiWave AirBuds Pro' from the available devices list.<br>Once connected, you will hear a confirmation tone.<br>Maintenance and Care<br>Clean earbuds regularly with a dry cloth.• <br>• <br>• <br>• <br>• <br>1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>• <br>Store in the charging case when not in use.<br>Avoid exposure to extreme temperatures and moisture.<br>Troubleshooting<br>Issue: Earbuds won't connect to device<br>Solution : Reset Bluetooth settings on your device and try<br>pairing again. If the problem persists, reset the earbuds by<br>holding the touch sensor on both buds for 10 seconds.<br>Issue: Poor audio quality<br>Solution : Ensure a strong Bluetooth connection and check for<br>obstructions. Also, try cleaning the speaker grills.<br>Issue: Battery life is shorter than expected<br>Solution : Fully discharge and then recharge the earbuds to<br>recalibrate battery capacity.<br>FAQs<br>Are the AirBuds Pro waterproof?<br>Yes, they have an IPX4 rating, meaning they are water-resistant<br>but not fully waterproof.<br>Can I use only one earbud at a time?<br>Yes, the AirBuds Pro supports mono mode.<br>Technical Specifications<br>Bluetooth Version : 5.2<br>Charging Port : USB-C<br>Weight : 5g per earbud<br>Driver Size : 10mm dynamic driver• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>Warranty and Support<br>The AcoustiWave AirBuds Pro come with a one-year limited warranty.<br>For support, visit our website or contact our customer service for live<br>assistance.<br>This documentation is intended as a comprehensive guide for users,<br>ensuring an optimal experience with the AcoustiWave AirBuds Pro.<br>For further assistance, please refer to the provided customer support<br>channels.|
# MAGIC |ActiveFit Elite Pro|ActiveFit Elite Pro Smartwatch<br>Documentation<br>Table of Contents<br>Introduction<br>Product Overview<br>Setup Instructions<br>Using the ActiveFit Elite Pro<br>Troubleshooting Guide<br>Maintenance and Care<br>Safety Information<br>Warranty and Support<br>1. Introduction<br>Welcome to the ActiveFit Elite Pro experience, where cutting-edge<br>technology meets uncompromised style. Designed for fitness<br>enthusiasts and tech aficionados alike, this smartwatch offers a<br>seamless blend of functionality and innovation.<br>2. Product Overview<br>Key Features:<br>Built-in GPS : Track your outdoor activities without the need to<br>carry your phone.<br>Heart Rate Monitor : Real-time monitoring for optimized health<br>tracking.<br>Water Resistance : Safe for swimming, with resistance up to 50<br>meters.1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>• <br>• <br>• <br>Bluetooth Connectivity : Easily pair with your smartphone to<br>receive notifications and sync data.<br>Long Battery Life : Enjoy up to 10 days of use on a single<br>charge.<br>Fast Charging : 50% charge in just 30 minutes.<br>Compatibility : Works with iOS and Android devices.<br>3. Setup Instructions<br>Step 1: Charging Your Device<br>Connect the magnetic charging cable to a power source.<br>Align the charger with the back of your watch until it clicks into<br>place.<br>Step 2: Device Pairing<br>Download the “ActiveFit” app from the App Store or Google Play.<br>Turn on Bluetooth on your phone and smartwatch.<br>Open the app and select ‘Pair New Device.’<br>Follow on-screen instructions to complete the pairing process.<br>4. Using the ActiveFit Elite Pro<br>Navigating the Interface<br>Swipe up to view notifications.<br>Swipe down for quick settings.<br>Swipe left or right to access apps and widgets.<br>Tracking Your Activity<br>Select the Activity app for options like running, cycling, or<br>swimming.<br>Start an activity to monitor metrics such as distance, pace, and<br>heart rate.• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>Customizing Your Watch Face<br>Long press on the current watch face.<br>Swipe through options and tap to select.<br>5. Troubleshooting Guide<br>Problem: App Keeps Crashing<br>Ensure the app is updated to the latest version.<br>Restart your phone and try opening the app again.<br>Clear app cache from phone settings.<br>Problem: Bluetooth Connectivity Issues<br>Make sure Bluetooth is enabled on both devices.<br>Forget the device in Bluetooth settings and re-pair.<br>Restart both devices if issues persist.<br>Problem: Stuck on Software Update<br>Restart the watch by holding the power button for 10 seconds.<br>If stuck, perform a factory reset by holding power and volume up<br>buttons.<br>6. Maintenance and Care<br>Battery Care : Avoid extreme temperatures and overcharging.<br>Cleaning : Use a damp cloth to clean the watch regularly.<br>Software Updates : Regularly check the app for firmware<br>updates.<br>7. Safety Information<br>Avoid exposure to high-velocity water.<br>Use only the provided charger to minimize risks of damage.• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>8. Warranty and Support<br>Warranty Coverage<br>The ActiveFit Elite Pro comes with a 1-year limited warranty<br>covering manufacturing defects.<br>Customer Support<br>Reach out via email at support@activefit.com or call our hotline<br>at 1-800-555-0199.<br>Thank you for choosing the ActiveFit Elite Pro. Elevate your lifestyle<br>with every step!• <br>•|
# MAGIC |Advanced Algebra_ Concepts and Applications|Product Overview<br>Advanced Algebra: Concepts and Applications  is designed to<br>enhance the learning experience for students studying algebra at an<br>advanced level. This textbook provides comprehensive coverage of<br>key algebraic concepts and real-world applications to prepare<br>students for higher education and careers in STEM fields.<br>Table of Contents<br>Introduction to Advanced Algebra<br>Polynomials and Factoring<br>Rational Equations and Functions<br>Exponential and Logarithmic Functions<br>Sequences and Series<br>Probability and Statistics<br>Matrices and Determinants<br>Additional Resources<br>Detailed Features<br>High-Quality Binding : Ensures durability for frequent use.<br>Digital Access Code : Included within the purchase for<br>accessing online modules and additional exercises.<br>Clear Illustrations : Diagrams and graphs support visual<br>understanding of complex concepts.<br>Supplementary Materials : Includes a downloadable study<br>guide and access to an online forum for peer interaction.<br>How to Purchase and Track Orders<br>Visit our website : Navigate to the 'T extbooks' section.<br>Select your edition : Choose from hardcover, paperback, or<br>digital format.1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>• <br>• <br>• <br>• <br>1. <br>2. <br>Checkout : Provide shipping information and select delivery<br>options.<br>Track Your Order : After purchasing, you'll receive a tracking<br>number via email.<br>Go to our website and enter your tracking ID on the 'Order<br>Status' page.<br>Opt in for SMS or email alerts for real-time updates about your<br>shipment.<br>Troubleshooting and FAQs<br>Q: What if my textbook is delayed?  A: Check your email for any<br>notifications or use our tracking portal. Contact customer support<br>with your order number for assistance.<br>Q: How do I access digital resources?  A: Use the access code<br>provided on the inside cover to register on our online portal.<br>Q: What should I do if I receive a defective book?  A: Contact<br>our support team immediately for a replacement. Please provide<br>photos of the defect and your order details.<br>Contact Information for Customer<br>Support<br>Our support team is available 24/7 to address any concerns. - Phone :<br>1-800-555-BOOK - Email : support@bookcube.com - Live Chat :<br>Available on our website<br>Appendices or Additional Resources<br>Answer Keys : Provided in a separate companion booklet for<br>instructors.<br>Study Guides : Downloadable content available with purchase.<br>Online Forum : Join our online community to discuss topics and<br>problem-solving strategies.3. <br>4. <br>5. <br>6. <br>• <br>• <br>• <br>By utilizing all the above features and support options, students can<br>fully maximize their learning while minimizing common issues.|

# COMMAND ----------

# DBTITLE 1,Step 3: Join Tables
# MAGIC %md
# MAGIC ## Step 3: Join Tables on Product Name
# MAGIC
# MAGIC Perform an inner join between products and product_docs using `product_name` as the key.

# COMMAND ----------

# DBTITLE 1,Join products and docs
# Join products with product_docs on product_name
joined_df = products_df.join(
    product_docs_df,
    on="product_name",
    how="inner"
)

print("Joined Table Schema:")
joined_df.printSchema()
print(f"\nTotal joined records: {joined_df.count()}")

# Display sample joined data
print("\nSample joined data:")
display(joined_df.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC Joined Table Schema:
# MAGIC root
# MAGIC  |-- product_name: string (nullable = true)
# MAGIC  |-- product_id: string (nullable = true)
# MAGIC  |-- product_category: string (nullable = true)
# MAGIC  |-- product_sub_category: string (nullable = true)
# MAGIC  |-- product_doc: string (nullable = true)
# MAGIC
# MAGIC
# MAGIC Total joined records: 550
# MAGIC
# MAGIC Sample joined data:
# MAGIC
# MAGIC |product_name|product_id|product_category|product_sub_category|product_doc|
# MAGIC |---|---|---|---|---|
# MAGIC |AccountEase Pro|498cd716-f5ad-4c55-94d1-38f78bcf16c2|Software|Online Platform|AccountEase Pro Documentation<br>Product Overview<br>AccountEase Pro is a robust online platform designed to simplify<br>personal account management for individuals and businesses alike.<br>With an intuitive interface, users can seamlessly manage their<br>accounts, reset passwords, and maintain high security standards.<br>Getting Started<br>Sign Up<br>Go to the AccountEase Pro website.<br>Click on the 'Sign Up' button.<br>Fill out the registration form with your name, email, and<br>password.<br>Confirm your registration via the verification email.<br>Login<br>Access the portal through the homepage.<br>Enter your registered email and password.<br>Click 'Login'.<br>Navigation<br>The dashboard features user-friendly tabs for quick access to<br>your profile, settings, and support.<br>Account Management<br>Resetting Your Password<br>Click on 'Forgot Password?' on the login page.<br>Enter your email address and submit the form.<br>Check your email for a reset link and follow the instructions.<br>Ensure the link is not expired (valid for 24 hours).<br>Updating Profile Information1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>9. <br>10. <br>11. <br>1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>Navigate to 'My Profile'.<br>Edit fields such as your name, email, and phone number.<br>Save changes.<br>Managing Security Settings<br>Go to 'Security Settings' in your account menu.<br>Enable two-factor authentication for enhanced security.<br>Review login history and change your password regularly.<br>Common Troubleshooting<br>Problem: Cannot Reset Password<br>Ensure the reset email hasn't gone to the spam folder.<br>Verify the email you entered is correct and registered.<br>If the link is not working, request a new reset link.<br>Problem: Email Verification Issues<br>Check for a verification email and follow the link inside.<br>Resend the verification email if not received within a few<br>minutes.<br>Problem: Account Locked<br>Accounts may lock after consecutive failed login attempts.<br>Contact support to unlock your account.<br>Advanced Features<br>Custom Integrations<br>Connect AccountEase Pro with third-party applications like<br>Google Drive or Dropbox.<br>Use API keys for seamless data flow.<br>User Customization<br>Personalize the dashboard theme and notification settings.7. <br>8. <br>9. <br>10. <br>11. <br>12. <br>13. <br>1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>9. <br>10. <br>1. <br>2. <br>3. <br>4. <br>5. <br>Support and Resources<br>FAQs<br>Visit our FAQ section for quick answers.<br>Customer Support<br>Reach out to support via live chat or email at<br>support@accounteasepro.com.<br>Useful Links<br>Tutorial Videos: www.accounteasepro.com/tutorials<br>User Forum: www.accounteasepro.com/forum<br>For further assistance, refer to our detailed support guides and video<br>tutorials available through the platform. Welcome to AccountEase Pro,<br>where managing your account is made easy and secure.• <br>• <br>• <br>• <br>• <br>• <br>•|
# MAGIC |AccuBooks Pro|54d5b355-5c1b-4584-b631-9994464117bd|Software|Accounting Software|AccuBooks Pro: Comprehensive User<br>Documentation<br>Table of Contents<br>Introduction<br>System Requirements<br>Installation Guide<br>Core Features<br>Getting Started<br>Troubleshooting<br>Frequently Asked Questions<br>T echnical Support<br>Updates and Maintenance<br>Compliance and Security<br>1. Introduction<br>AccuBooks Pro is a cutting-edge accounting software designed for<br>small to medium-sized businesses. With a user-friendly interface and<br>robust functionality, it streamlines financial management tasks such<br>as invoicing, ledger maintenance, tax compliance, payroll, and<br>customized reporting.<br>2. System Requirements<br>Operating System : Windows 10 or later, macOS 10.14 or later<br>Processor : 1 GHz or faster<br>Memory : 8 GB RAM<br>Storage : 500 MB of available hard disk space<br>Internet : Broadband connection for updates and cloud features<br>3. Installation Guide<br>Step 1:  Download the AccuBooks Pro installer from our official<br>website.1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>9. <br>10. <br>• <br>• <br>• <br>• <br>• <br>Step 2:  Double-click the downloaded file to begin the installation.<br>Step 3:  Follow the on-screen instructions to complete the setup.<br>Step 4:  Enter your license key when prompted to activate the<br>software.<br>Common Installation Issues:  - Error 404 : Re-download the<br>installer as this error indicates a corrupted file. - Error 302 : Ensure<br>your operating system is up-to-date before re-attempting installation.<br>4. Core Features<br>Transaction Management : Record and categorize financial<br>transactions with ease.<br>Invoicing : Create and send professional invoices directly from<br>the software.<br>Reporting : Generate detailed financial reports for better<br>analysis and strategic planning.<br>Payroll Management : Simplify payroll processing with<br>automatic tax calculations and payslip generation.<br>5. Getting Started<br>Creating a New Account:  - Navigate to the Accounts module. -<br>Click 'New Account' and follow the guided setup process.<br>Entering Transactions:  - Access the Transactions module. - Select<br>'New Transaction' and input the relevant details.<br>Generating Reports:  - Go to the Reports section. - Choose the type<br>of report and customize the parameters as needed.<br>6. Troubleshooting<br>Software Crashes During Report Generation:  Use the web<br>version as an alternative; ensure you have the latest software<br>update.<br>Installation Issues:  Verify system compatibility and consult the<br>installation issues section for specific error codes.• <br>• <br>• <br>• <br>7. Frequently Asked Questions<br>Q: How do I recover lost data?  A: Navigate to the Backup &<br>Restore section under Settings to explore recovery options.<br>Q: Can I integrate external applications?  A: Yes, AccuBooks<br>Pro supports various third-party integrations. Visit our website<br>for a list of available integrations.<br>8. Technical Support<br>For additional support, please contact our technical team: - Email:<br>support@accubookspro.com - Phone:  1-800-555-0199<br>9. Updates and Maintenance<br>Regularly check for software updates under the Help menu to ensure<br>optimal performance. It is advised to perform backups before major<br>updates.<br>10. Compliance and Security<br>AccuBooks Pro ensures compliance with regional tax regulations and<br>incorporates robust security features such as data encryption, multi-<br>factor authentication, and audit trails.<br>For further details, refer to the in-app Help section or visit our online<br>knowledge base.• <br>•|
# MAGIC |AcoustiWave AirBuds Pro|8dfeb92d-fc46-4f01-b99e-14c05c7281e7|Accessories|Wireless Earbuds|AcoustiWave AirBuds Pro Documentation<br>Product Overview<br>The AcoustiWave AirBuds Pro are premium wireless earbuds designed<br>for superior sound and seamless connectivity. Featuring advanced<br>Bluetooth 5.2 technology, these earbuds provide a high-fidelity audio<br>experience with minimal latency, making them perfect for music<br>lovers and on-the-go professionals.<br>Key Features<br>Bluetooth 5.2 Connectivity : Ensures a stable and fast<br>connection with all Bluetooth-enabled devices.<br>Voice Assistant Support : Compatible with Siri and Google<br>Assistant for hands-free control.<br>Noise Cancellation : Active Noise Cancellation (ANC) for an<br>immersive listening experience.<br>Battery Life : Provides up to 7 hours of playtime on a single<br>charge, with an additional 21 hours from the charging case.<br>Ergonomic Design : Lightweight and comfortable fit for<br>extended wear.<br>Getting Started<br>Charging : Fully charge the earbuds and the case using the<br>included USB-C cable.<br>Power On : Remove the earbuds from the case; they will<br>automatically power on.<br>Pairing :<br>Turn on Bluetooth on your device.<br>Select 'AcoustiWave AirBuds Pro' from the available devices list.<br>Once connected, you will hear a confirmation tone.<br>Maintenance and Care<br>Clean earbuds regularly with a dry cloth.• <br>• <br>• <br>• <br>• <br>1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>• <br>Store in the charging case when not in use.<br>Avoid exposure to extreme temperatures and moisture.<br>Troubleshooting<br>Issue: Earbuds won't connect to device<br>Solution : Reset Bluetooth settings on your device and try<br>pairing again. If the problem persists, reset the earbuds by<br>holding the touch sensor on both buds for 10 seconds.<br>Issue: Poor audio quality<br>Solution : Ensure a strong Bluetooth connection and check for<br>obstructions. Also, try cleaning the speaker grills.<br>Issue: Battery life is shorter than expected<br>Solution : Fully discharge and then recharge the earbuds to<br>recalibrate battery capacity.<br>FAQs<br>Are the AirBuds Pro waterproof?<br>Yes, they have an IPX4 rating, meaning they are water-resistant<br>but not fully waterproof.<br>Can I use only one earbud at a time?<br>Yes, the AirBuds Pro supports mono mode.<br>Technical Specifications<br>Bluetooth Version : 5.2<br>Charging Port : USB-C<br>Weight : 5g per earbud<br>Driver Size : 10mm dynamic driver• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>Warranty and Support<br>The AcoustiWave AirBuds Pro come with a one-year limited warranty.<br>For support, visit our website or contact our customer service for live<br>assistance.<br>This documentation is intended as a comprehensive guide for users,<br>ensuring an optimal experience with the AcoustiWave AirBuds Pro.<br>For further assistance, please refer to the provided customer support<br>channels.|
# MAGIC

# COMMAND ----------

# DBTITLE 1,Step 4: Create Indexed Document Column
# MAGIC %md
# MAGIC ## Step 4: Create Indexed Document Column
# MAGIC
# MAGIC Create the `indexed_doc` column with XML-style formatting that includes:
# MAGIC - Product category
# MAGIC - Product sub-category
# MAGIC - Product name
# MAGIC - Product documentation
# MAGIC
# MAGIC This format is optimized for downstream LLM processing and retrieval.

# COMMAND ----------

# DBTITLE 1,Create indexed_doc column
# Create the indexed_doc column with XML-style tags
indexed_df = joined_df.withColumn(
    "indexed_doc",
    F.concat(
        F.lit("<product_category>"),
        F.col("product_category"),
        F.lit("</product_category>\n"),
        F.lit("<product_sub_category>"),
        F.col("product_sub_category"),
        F.lit("</product_sub_category>\n"),
        F.lit("<product_name>"),
        F.col("product_name"),
        F.lit("</product_name>\n"),
        F.lit("<product_doc>\n"),
        F.col("product_doc"),
        F.lit("\n</product_doc>")
    )
)

print("✓ Indexed document column created")

# Display sample indexed document
print("\nSample indexed document (first 500 characters):")
sample_indexed = indexed_df.select("product_name", "indexed_doc").first()
print(f"\nProduct: {sample_indexed['product_name']}")
print(f"\nIndexed Doc:\n{sample_indexed['indexed_doc'][:500]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ✓ Indexed document column created
# MAGIC
# MAGIC Sample indexed document (first 500 characters):
# MAGIC
# MAGIC Product: AccountEase Pro
# MAGIC
# MAGIC Indexed Doc:
# MAGIC <product_category>Software</product_category>
# MAGIC <product_sub_category>Online Platform</product_sub_category>
# MAGIC <product_name>AccountEase Pro</product_name>
# MAGIC <product_doc>
# MAGIC AccountEase Pro Documentation
# MAGIC Product Overview
# MAGIC AccountEase Pro is a robust online platform designed to simplify
# MAGIC personal account management for individuals and businesses alike.
# MAGIC With an intuitive interface, users can seamlessly manage their
# MAGIC accounts, reset passwords, and maintain high security standards.
# MAGIC Getting Started
# MAGIC Sign Up
# MAGIC Go t...

# COMMAND ----------

# DBTITLE 1,Step 5: Select Final Columns
# MAGIC %md
# MAGIC ## Step 5: Select Final Columns
# MAGIC
# MAGIC Select and reorder columns for the final table structure.

# COMMAND ----------

# DBTITLE 1,Select final columns
# Select the required columns in the specified order
final_df = indexed_df.select(
    "product_id",
    "product_name",
    "product_doc",
    "product_category",
    "product_sub_category",
    "indexed_doc"
)

print("Final Table Schema:")
final_df.printSchema()
print(f"\nTotal records: {final_df.count()}")

# Display final sample
print("\nSample final data:")
display(final_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Final Table Schema:
# MAGIC root
# MAGIC  |-- product_id: string (nullable = true)
# MAGIC  |-- product_name: string (nullable = true)
# MAGIC  |-- product_doc: string (nullable = true)
# MAGIC  |-- product_category: string (nullable = true)
# MAGIC  |-- product_sub_category: string (nullable = true)
# MAGIC  |-- indexed_doc: string (nullable = true)
# MAGIC
# MAGIC
# MAGIC Total records: 550
# MAGIC
# MAGIC Sample final data:
# MAGIC
# MAGIC product_id	product_name	product_doc	product_category	product_sub_category	indexed_doc

# COMMAND ----------

# DBTITLE 1,Step 6: Save to New Table
# MAGIC %md
# MAGIC ## Step 6: Save to Unity Catalog Table
# MAGIC
# MAGIC Save the indexed products to a new managed table in Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Save to table
# Define the target table name
target_table = "agentic_catalog.agentic_schema.product_docs_combined"

# Write the DataFrame to Unity Catalog as a managed Delta table
# Using 'overwrite' mode to replace if exists
final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(target_table)

print(f"✓ Table saved successfully: {target_table}")
print(f"\nTotal records written: {spark.table(target_table).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ✓ Table saved successfully: agentic_catalog.agentic_schema.product_docs_combined
# MAGIC
# MAGIC Total records written: 550

# COMMAND ----------

# DBTITLE 1,Step 7: Verify Results
# MAGIC %md
# MAGIC ## Step 7: Verify the Created Table
# MAGIC
# MAGIC Query the newly created table to verify the data and structure.

# COMMAND ----------

# DBTITLE 1,Verify table
# Read the table back and verify
verify_df = spark.table("agentic_catalog.agentic_schema.product_docs_combined")

print("Verification Results:")
print(f"Total records: {verify_df.count()}")
print("\nTable Schema:")
verify_df.printSchema()

# Display sample records
print("\nSample Records:")
display(verify_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Verification Results:
# MAGIC Total records: 550
# MAGIC
# MAGIC Table Schema:
# MAGIC root
# MAGIC  |-- product_id: string (nullable = true)
# MAGIC  |-- product_name: string (nullable = true)
# MAGIC  |-- product_doc: string (nullable = true)
# MAGIC  |-- product_category: string (nullable = true)
# MAGIC  |-- product_sub_category: string (nullable = true)
# MAGIC  |-- indexed_doc: string (nullable = true)
# MAGIC
# MAGIC
# MAGIC Sample Records:
# MAGIC |product_id|product_name|product_doc|product_category|product_sub_category|indexed_doc|
# MAGIC |---|---|---|---|---|---|
# MAGIC |498cd716-f5ad-4c55-94d1-38f78bcf16c2|AccountEase Pro|AccountEase Pro Documentation<br>Product Overview<br>AccountEase Pro is a robust online platform designed to simplify<br>personal account management for individuals and businesses alike.<br>With an intuitive interface, users can seamlessly manage their<br>accounts, reset passwords, and maintain high security standards.<br>Getting Started<br>Sign Up<br>Go to the AccountEase Pro website.<br>Click on the 'Sign Up' button.<br>Fill out the registration form with your name, email, and<br>password.<br>Confirm your registration via the verification email.<br>Login<br>Access the portal through the homepage.<br>Enter your registered email and password.<br>Click 'Login'.<br>Navigation<br>The dashboard features user-friendly tabs for quick access to<br>your profile, settings, and support.<br>Account Management<br>Resetting Your Password<br>Click on 'Forgot Password?' on the login page.<br>Enter your email address and submit the form.<br>Check your email for a reset link and follow the instructions.<br>Ensure the link is not expired (valid for 24 hours).<br>Updating Profile Information1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>9. <br>10. <br>11. <br>1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>Navigate to 'My Profile'.<br>Edit fields such as your name, email, and phone number.<br>Save changes.<br>Managing Security Settings<br>Go to 'Security Settings' in your account menu.<br>Enable two-factor authentication for enhanced security.<br>Review login history and change your password regularly.<br>Common Troubleshooting<br>Problem: Cannot Reset Password<br>Ensure the reset email hasn't gone to the spam folder.<br>Verify the email you entered is correct and registered.<br>If the link is not working, request a new reset link.<br>Problem: Email Verification Issues<br>Check for a verification email and follow the link inside.<br>Resend the verification email if not received within a few<br>minutes.<br>Problem: Account Locked<br>Accounts may lock after consecutive failed login attempts.<br>Contact support to unlock your account.<br>Advanced Features<br>Custom Integrations<br>Connect AccountEase Pro with third-party applications like<br>Google Drive or Dropbox.<br>Use API keys for seamless data flow.<br>User Customization<br>Personalize the dashboard theme and notification settings.7. <br>8. <br>9. <br>10. <br>11. <br>12. <br>13. <br>1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>9. <br>10. <br>1. <br>2. <br>3. <br>4. <br>5. <br>Support and Resources<br>FAQs<br>Visit our FAQ section for quick answers.<br>Customer Support<br>Reach out to support via live chat or email at<br>support@accounteasepro.com.<br>Useful Links<br>Tutorial Videos: www.accounteasepro.com/tutorials<br>User Forum: www.accounteasepro.com/forum<br>For further assistance, refer to our detailed support guides and video<br>tutorials available through the platform. Welcome to AccountEase Pro,<br>where managing your account is made easy and secure.• <br>• <br>• <br>• <br>• <br>• <br>•|Software|Online Platform|<product_category>Software</product_category><br><product_sub_category>Online Platform</product_sub_category><br><product_name>AccountEase Pro</product_name><br><product_doc><br>AccountEase Pro Documentation<br>Product Overview<br>AccountEase Pro is a robust online platform designed to simplify<br>personal account management for individuals and businesses alike.<br>With an intuitive interface, users can seamlessly manage their<br>accounts, reset passwords, and maintain high security standards.<br>Getting Started<br>Sign Up<br>Go to the AccountEase Pro website.<br>Click on the 'Sign Up' button.<br>Fill out the registration form with your name, email, and<br>password.<br>Confirm your registration via the verification email.<br>Login<br>Access the portal through the homepage.<br>Enter your registered email and password.<br>Click 'Login'.<br>Navigation<br>The dashboard features user-friendly tabs for quick access to<br>your profile, settings, and support.<br>Account Management<br>Resetting Your Password<br>Click on 'Forgot Password?' on the login page.<br>Enter your email address and submit the form.<br>Check your email for a reset link and follow the instructions.<br>Ensure the link is not expired (valid for 24 hours).<br>Updating Profile Information1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>9. <br>10. <br>11. <br>1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>Navigate to 'My Profile'.<br>Edit fields such as your name, email, and phone number.<br>Save changes.<br>Managing Security Settings<br>Go to 'Security Settings' in your account menu.<br>Enable two-factor authentication for enhanced security.<br>Review login history and change your password regularly.<br>Common Troubleshooting<br>Problem: Cannot Reset Password<br>Ensure the reset email hasn't gone to the spam folder.<br>Verify the email you entered is correct and registered.<br>If the link is not working, request a new reset link.<br>Problem: Email Verification Issues<br>Check for a verification email and follow the link inside.<br>Resend the verification email if not received within a few<br>minutes.<br>Problem: Account Locked<br>Accounts may lock after consecutive failed login attempts.<br>Contact support to unlock your account.<br>Advanced Features<br>Custom Integrations<br>Connect AccountEase Pro with third-party applications like<br>Google Drive or Dropbox.<br>Use API keys for seamless data flow.<br>User Customization<br>Personalize the dashboard theme and notification settings.7. <br>8. <br>9. <br>10. <br>11. <br>12. <br>13. <br>1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>9. <br>10. <br>1. <br>2. <br>3. <br>4. <br>5. <br>Support and Resources<br>FAQs<br>Visit our FAQ section for quick answers.<br>Customer Support<br>Reach out to support via live chat or email at<br>support@accounteasepro.com.<br>Useful Links<br>Tutorial Videos: www.accounteasepro.com/tutorials<br>User Forum: www.accounteasepro.com/forum<br>For further assistance, refer to our detailed support guides and video<br>tutorials available through the platform. Welcome to AccountEase Pro,<br>where managing your account is made easy and secure.• <br>• <br>• <br>• <br>• <br>• <br>•<br></product_doc>|
# MAGIC |54d5b355-5c1b-4584-b631-9994464117bd|AccuBooks Pro|AccuBooks Pro: Comprehensive User<br>Documentation<br>Table of Contents<br>Introduction<br>System Requirements<br>Installation Guide<br>Core Features<br>Getting Started<br>Troubleshooting<br>Frequently Asked Questions<br>T echnical Support<br>Updates and Maintenance<br>Compliance and Security<br>1. Introduction<br>AccuBooks Pro is a cutting-edge accounting software designed for<br>small to medium-sized businesses. With a user-friendly interface and<br>robust functionality, it streamlines financial management tasks such<br>as invoicing, ledger maintenance, tax compliance, payroll, and<br>customized reporting.<br>2. System Requirements<br>Operating System : Windows 10 or later, macOS 10.14 or later<br>Processor : 1 GHz or faster<br>Memory : 8 GB RAM<br>Storage : 500 MB of available hard disk space<br>Internet : Broadband connection for updates and cloud features<br>3. Installation Guide<br>Step 1:  Download the AccuBooks Pro installer from our official<br>website.1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>9. <br>10. <br>• <br>• <br>• <br>• <br>• <br>Step 2:  Double-click the downloaded file to begin the installation.<br>Step 3:  Follow the on-screen instructions to complete the setup.<br>Step 4:  Enter your license key when prompted to activate the<br>software.<br>Common Installation Issues:  - Error 404 : Re-download the<br>installer as this error indicates a corrupted file. - Error 302 : Ensure<br>your operating system is up-to-date before re-attempting installation.<br>4. Core Features<br>Transaction Management : Record and categorize financial<br>transactions with ease.<br>Invoicing : Create and send professional invoices directly from<br>the software.<br>Reporting : Generate detailed financial reports for better<br>analysis and strategic planning.<br>Payroll Management : Simplify payroll processing with<br>automatic tax calculations and payslip generation.<br>5. Getting Started<br>Creating a New Account:  - Navigate to the Accounts module. -<br>Click 'New Account' and follow the guided setup process.<br>Entering Transactions:  - Access the Transactions module. - Select<br>'New Transaction' and input the relevant details.<br>Generating Reports:  - Go to the Reports section. - Choose the type<br>of report and customize the parameters as needed.<br>6. Troubleshooting<br>Software Crashes During Report Generation:  Use the web<br>version as an alternative; ensure you have the latest software<br>update.<br>Installation Issues:  Verify system compatibility and consult the<br>installation issues section for specific error codes.• <br>• <br>• <br>• <br>7. Frequently Asked Questions<br>Q: How do I recover lost data?  A: Navigate to the Backup &<br>Restore section under Settings to explore recovery options.<br>Q: Can I integrate external applications?  A: Yes, AccuBooks<br>Pro supports various third-party integrations. Visit our website<br>for a list of available integrations.<br>8. Technical Support<br>For additional support, please contact our technical team: - Email:<br>support@accubookspro.com - Phone:  1-800-555-0199<br>9. Updates and Maintenance<br>Regularly check for software updates under the Help menu to ensure<br>optimal performance. It is advised to perform backups before major<br>updates.<br>10. Compliance and Security<br>AccuBooks Pro ensures compliance with regional tax regulations and<br>incorporates robust security features such as data encryption, multi-<br>factor authentication, and audit trails.<br>For further details, refer to the in-app Help section or visit our online<br>knowledge base.• <br>•|Software|Accounting Software|<product_category>Software</product_category><br><product_sub_category>Accounting Software</product_sub_category><br><product_name>AccuBooks Pro</product_name><br><product_doc><br>AccuBooks Pro: Comprehensive User<br>Documentation<br>Table of Contents<br>Introduction<br>System Requirements<br>Installation Guide<br>Core Features<br>Getting Started<br>Troubleshooting<br>Frequently Asked Questions<br>T echnical Support<br>Updates and Maintenance<br>Compliance and Security<br>1. Introduction<br>AccuBooks Pro is a cutting-edge accounting software designed for<br>small to medium-sized businesses. With a user-friendly interface and<br>robust functionality, it streamlines financial management tasks such<br>as invoicing, ledger maintenance, tax compliance, payroll, and<br>customized reporting.<br>2. System Requirements<br>Operating System : Windows 10 or later, macOS 10.14 or later<br>Processor : 1 GHz or faster<br>Memory : 8 GB RAM<br>Storage : 500 MB of available hard disk space<br>Internet : Broadband connection for updates and cloud features<br>3. Installation Guide<br>Step 1:  Download the AccuBooks Pro installer from our official<br>website.1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>9. <br>10. <br>• <br>• <br>• <br>• <br>• <br>Step 2:  Double-click the downloaded file to begin the installation.<br>Step 3:  Follow the on-screen instructions to complete the setup.<br>Step 4:  Enter your license key when prompted to activate the<br>software.<br>Common Installation Issues:  - Error 404 : Re-download the<br>installer as this error indicates a corrupted file. - Error 302 : Ensure<br>your operating system is up-to-date before re-attempting installation.<br>4. Core Features<br>Transaction Management : Record and categorize financial<br>transactions with ease.<br>Invoicing : Create and send professional invoices directly from<br>the software.<br>Reporting : Generate detailed financial reports for better<br>analysis and strategic planning.<br>Payroll Management : Simplify payroll processing with<br>automatic tax calculations and payslip generation.<br>5. Getting Started<br>Creating a New Account:  - Navigate to the Accounts module. -<br>Click 'New Account' and follow the guided setup process.<br>Entering Transactions:  - Access the Transactions module. - Select<br>'New Transaction' and input the relevant details.<br>Generating Reports:  - Go to the Reports section. - Choose the type<br>of report and customize the parameters as needed.<br>6. Troubleshooting<br>Software Crashes During Report Generation:  Use the web<br>version as an alternative; ensure you have the latest software<br>update.<br>Installation Issues:  Verify system compatibility and consult the<br>installation issues section for specific error codes.• <br>• <br>• <br>• <br>7. Frequently Asked Questions<br>Q: How do I recover lost data?  A: Navigate to the Backup &<br>Restore section under Settings to explore recovery options.<br>Q: Can I integrate external applications?  A: Yes, AccuBooks<br>Pro supports various third-party integrations. Visit our website<br>for a list of available integrations.<br>8. Technical Support<br>For additional support, please contact our technical team: - Email:<br>support@accubookspro.com - Phone:  1-800-555-0199<br>9. Updates and Maintenance<br>Regularly check for software updates under the Help menu to ensure<br>optimal performance. It is advised to perform backups before major<br>updates.<br>10. Compliance and Security<br>AccuBooks Pro ensures compliance with regional tax regulations and<br>incorporates robust security features such as data encryption, multi-<br>factor authentication, and audit trails.<br>For further details, refer to the in-app Help section or visit our online<br>knowledge base.• <br>•<br></product_doc>|
# MAGIC |8dfeb92d-fc46-4f01-b99e-14c05c7281e7|AcoustiWave AirBuds Pro|AcoustiWave AirBuds Pro Documentation<br>Product Overview<br>The AcoustiWave AirBuds Pro are premium wireless earbuds designed<br>for superior sound and seamless connectivity. Featuring advanced<br>Bluetooth 5.2 technology, these earbuds provide a high-fidelity audio<br>experience with minimal latency, making them perfect for music<br>lovers and on-the-go professionals.<br>Key Features<br>Bluetooth 5.2 Connectivity : Ensures a stable and fast<br>connection with all Bluetooth-enabled devices.<br>Voice Assistant Support : Compatible with Siri and Google<br>Assistant for hands-free control.<br>Noise Cancellation : Active Noise Cancellation (ANC) for an<br>immersive listening experience.<br>Battery Life : Provides up to 7 hours of playtime on a single<br>charge, with an additional 21 hours from the charging case.<br>Ergonomic Design : Lightweight and comfortable fit for<br>extended wear.<br>Getting Started<br>Charging : Fully charge the earbuds and the case using the<br>included USB-C cable.<br>Power On : Remove the earbuds from the case; they will<br>automatically power on.<br>Pairing :<br>Turn on Bluetooth on your device.<br>Select 'AcoustiWave AirBuds Pro' from the available devices list.<br>Once connected, you will hear a confirmation tone.<br>Maintenance and Care<br>Clean earbuds regularly with a dry cloth.• <br>• <br>• <br>• <br>• <br>1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>• <br>Store in the charging case when not in use.<br>Avoid exposure to extreme temperatures and moisture.<br>Troubleshooting<br>Issue: Earbuds won't connect to device<br>Solution : Reset Bluetooth settings on your device and try<br>pairing again. If the problem persists, reset the earbuds by<br>holding the touch sensor on both buds for 10 seconds.<br>Issue: Poor audio quality<br>Solution : Ensure a strong Bluetooth connection and check for<br>obstructions. Also, try cleaning the speaker grills.<br>Issue: Battery life is shorter than expected<br>Solution : Fully discharge and then recharge the earbuds to<br>recalibrate battery capacity.<br>FAQs<br>Are the AirBuds Pro waterproof?<br>Yes, they have an IPX4 rating, meaning they are water-resistant<br>but not fully waterproof.<br>Can I use only one earbud at a time?<br>Yes, the AirBuds Pro supports mono mode.<br>Technical Specifications<br>Bluetooth Version : 5.2<br>Charging Port : USB-C<br>Weight : 5g per earbud<br>Driver Size : 10mm dynamic driver• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>Warranty and Support<br>The AcoustiWave AirBuds Pro come with a one-year limited warranty.<br>For support, visit our website or contact our customer service for live<br>assistance.<br>This documentation is intended as a comprehensive guide for users,<br>ensuring an optimal experience with the AcoustiWave AirBuds Pro.<br>For further assistance, please refer to the provided customer support<br>channels.|Accessories|Wireless Earbuds|<product_category>Accessories</product_category><br><product_sub_category>Wireless Earbuds</product_sub_category><br><product_name>AcoustiWave AirBuds Pro</product_name><br><product_doc><br>AcoustiWave AirBuds Pro Documentation<br>Product Overview<br>The AcoustiWave AirBuds Pro are premium wireless earbuds designed<br>for superior sound and seamless connectivity. Featuring advanced<br>Bluetooth 5.2 technology, these earbuds provide a high-fidelity audio<br>experience with minimal latency, making them perfect for music<br>lovers and on-the-go professionals.<br>Key Features<br>Bluetooth 5.2 Connectivity : Ensures a stable and fast<br>connection with all Bluetooth-enabled devices.<br>Voice Assistant Support : Compatible with Siri and Google<br>Assistant for hands-free control.<br>Noise Cancellation : Active Noise Cancellation (ANC) for an<br>immersive listening experience.<br>Battery Life : Provides up to 7 hours of playtime on a single<br>charge, with an additional 21 hours from the charging case.<br>Ergonomic Design : Lightweight and comfortable fit for<br>extended wear.<br>Getting Started<br>Charging : Fully charge the earbuds and the case using the<br>included USB-C cable.<br>Power On : Remove the earbuds from the case; they will<br>automatically power on.<br>Pairing :<br>Turn on Bluetooth on your device.<br>Select 'AcoustiWave AirBuds Pro' from the available devices list.<br>Once connected, you will hear a confirmation tone.<br>Maintenance and Care<br>Clean earbuds regularly with a dry cloth.• <br>• <br>• <br>• <br>• <br>1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>• <br>Store in the charging case when not in use.<br>Avoid exposure to extreme temperatures and moisture.<br>Troubleshooting<br>Issue: Earbuds won't connect to device<br>Solution : Reset Bluetooth settings on your device and try<br>pairing again. If the problem persists, reset the earbuds by<br>holding the touch sensor on both buds for 10 seconds.<br>Issue: Poor audio quality<br>Solution : Ensure a strong Bluetooth connection and check for<br>obstructions. Also, try cleaning the speaker grills.<br>Issue: Battery life is shorter than expected<br>Solution : Fully discharge and then recharge the earbuds to<br>recalibrate battery capacity.<br>FAQs<br>Are the AirBuds Pro waterproof?<br>Yes, they have an IPX4 rating, meaning they are water-resistant<br>but not fully waterproof.<br>Can I use only one earbud at a time?<br>Yes, the AirBuds Pro supports mono mode.<br>Technical Specifications<br>Bluetooth Version : 5.2<br>Charging Port : USB-C<br>Weight : 5g per earbud<br>Driver Size : 10mm dynamic driver• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>Warranty and Support<br>The AcoustiWave AirBuds Pro come with a one-year limited warranty.<br>For support, visit our website or contact our customer service for live<br>assistance.<br>This documentation is intended as a comprehensive guide for users,<br>ensuring an optimal experience with the AcoustiWave AirBuds Pro.<br>For further assistance, please refer to the provided customer support<br>channels.<br></product_doc>|
# MAGIC |1be746e9-859c-42a1-a79b-9b71b81fd128|ActiveFit Elite Pro|ActiveFit Elite Pro Smartwatch<br>Documentation<br>Table of Contents<br>Introduction<br>Product Overview<br>Setup Instructions<br>Using the ActiveFit Elite Pro<br>Troubleshooting Guide<br>Maintenance and Care<br>Safety Information<br>Warranty and Support<br>1. Introduction<br>Welcome to the ActiveFit Elite Pro experience, where cutting-edge<br>technology meets uncompromised style. Designed for fitness<br>enthusiasts and tech aficionados alike, this smartwatch offers a<br>seamless blend of functionality and innovation.<br>2. Product Overview<br>Key Features:<br>Built-in GPS : Track your outdoor activities without the need to<br>carry your phone.<br>Heart Rate Monitor : Real-time monitoring for optimized health<br>tracking.<br>Water Resistance : Safe for swimming, with resistance up to 50<br>meters.1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>• <br>• <br>• <br>Bluetooth Connectivity : Easily pair with your smartphone to<br>receive notifications and sync data.<br>Long Battery Life : Enjoy up to 10 days of use on a single<br>charge.<br>Fast Charging : 50% charge in just 30 minutes.<br>Compatibility : Works with iOS and Android devices.<br>3. Setup Instructions<br>Step 1: Charging Your Device<br>Connect the magnetic charging cable to a power source.<br>Align the charger with the back of your watch until it clicks into<br>place.<br>Step 2: Device Pairing<br>Download the “ActiveFit” app from the App Store or Google Play.<br>Turn on Bluetooth on your phone and smartwatch.<br>Open the app and select ‘Pair New Device.’<br>Follow on-screen instructions to complete the pairing process.<br>4. Using the ActiveFit Elite Pro<br>Navigating the Interface<br>Swipe up to view notifications.<br>Swipe down for quick settings.<br>Swipe left or right to access apps and widgets.<br>Tracking Your Activity<br>Select the Activity app for options like running, cycling, or<br>swimming.<br>Start an activity to monitor metrics such as distance, pace, and<br>heart rate.• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>Customizing Your Watch Face<br>Long press on the current watch face.<br>Swipe through options and tap to select.<br>5. Troubleshooting Guide<br>Problem: App Keeps Crashing<br>Ensure the app is updated to the latest version.<br>Restart your phone and try opening the app again.<br>Clear app cache from phone settings.<br>Problem: Bluetooth Connectivity Issues<br>Make sure Bluetooth is enabled on both devices.<br>Forget the device in Bluetooth settings and re-pair.<br>Restart both devices if issues persist.<br>Problem: Stuck on Software Update<br>Restart the watch by holding the power button for 10 seconds.<br>If stuck, perform a factory reset by holding power and volume up<br>buttons.<br>6. Maintenance and Care<br>Battery Care : Avoid extreme temperatures and overcharging.<br>Cleaning : Use a damp cloth to clean the watch regularly.<br>Software Updates : Regularly check the app for firmware<br>updates.<br>7. Safety Information<br>Avoid exposure to high-velocity water.<br>Use only the provided charger to minimize risks of damage.• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>8. Warranty and Support<br>Warranty Coverage<br>The ActiveFit Elite Pro comes with a 1-year limited warranty<br>covering manufacturing defects.<br>Customer Support<br>Reach out via email at support@activefit.com or call our hotline<br>at 1-800-555-0199.<br>Thank you for choosing the ActiveFit Elite Pro. Elevate your lifestyle<br>with every step!• <br>•|Wearables|Smartwatch|<product_category>Wearables</product_category><br><product_sub_category>Smartwatch</product_sub_category><br><product_name>ActiveFit Elite Pro</product_name><br><product_doc><br>ActiveFit Elite Pro Smartwatch<br>Documentation<br>Table of Contents<br>Introduction<br>Product Overview<br>Setup Instructions<br>Using the ActiveFit Elite Pro<br>Troubleshooting Guide<br>Maintenance and Care<br>Safety Information<br>Warranty and Support<br>1. Introduction<br>Welcome to the ActiveFit Elite Pro experience, where cutting-edge<br>technology meets uncompromised style. Designed for fitness<br>enthusiasts and tech aficionados alike, this smartwatch offers a<br>seamless blend of functionality and innovation.<br>2. Product Overview<br>Key Features:<br>Built-in GPS : Track your outdoor activities without the need to<br>carry your phone.<br>Heart Rate Monitor : Real-time monitoring for optimized health<br>tracking.<br>Water Resistance : Safe for swimming, with resistance up to 50<br>meters.1. <br>2. <br>3. <br>4. <br>5. <br>6. <br>7. <br>8. <br>• <br>• <br>• <br>Bluetooth Connectivity : Easily pair with your smartphone to<br>receive notifications and sync data.<br>Long Battery Life : Enjoy up to 10 days of use on a single<br>charge.<br>Fast Charging : 50% charge in just 30 minutes.<br>Compatibility : Works with iOS and Android devices.<br>3. Setup Instructions<br>Step 1: Charging Your Device<br>Connect the magnetic charging cable to a power source.<br>Align the charger with the back of your watch until it clicks into<br>place.<br>Step 2: Device Pairing<br>Download the “ActiveFit” app from the App Store or Google Play.<br>Turn on Bluetooth on your phone and smartwatch.<br>Open the app and select ‘Pair New Device.’<br>Follow on-screen instructions to complete the pairing process.<br>4. Using the ActiveFit Elite Pro<br>Navigating the Interface<br>Swipe up to view notifications.<br>Swipe down for quick settings.<br>Swipe left or right to access apps and widgets.<br>Tracking Your Activity<br>Select the Activity app for options like running, cycling, or<br>swimming.<br>Start an activity to monitor metrics such as distance, pace, and<br>heart rate.• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>Customizing Your Watch Face<br>Long press on the current watch face.<br>Swipe through options and tap to select.<br>5. Troubleshooting Guide<br>Problem: App Keeps Crashing<br>Ensure the app is updated to the latest version.<br>Restart your phone and try opening the app again.<br>Clear app cache from phone settings.<br>Problem: Bluetooth Connectivity Issues<br>Make sure Bluetooth is enabled on both devices.<br>Forget the device in Bluetooth settings and re-pair.<br>Restart both devices if issues persist.<br>Problem: Stuck on Software Update<br>Restart the watch by holding the power button for 10 seconds.<br>If stuck, perform a factory reset by holding power and volume up<br>buttons.<br>6. Maintenance and Care<br>Battery Care : Avoid extreme temperatures and overcharging.<br>Cleaning : Use a damp cloth to clean the watch regularly.<br>Software Updates : Regularly check the app for firmware<br>updates.<br>7. Safety Information<br>Avoid exposure to high-velocity water.<br>Use only the provided charger to minimize risks of damage.• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>8. Warranty and Support<br>Warranty Coverage<br>The ActiveFit Elite Pro comes with a 1-year limited warranty<br>covering manufacturing defects.<br>Customer Support<br>Reach out via email at support@activefit.com or call our hotline<br>at 1-800-555-0199.<br>Thank you for choosing the ActiveFit Elite Pro. Elevate your lifestyle<br>with every step!• <br>•<br></product_doc>|
# MAGIC |50238167-d6ce-452a-8267-ee41d13bd4f3|AirPure Elite 7000|AirPure Elite 7000 Product<br>Documentation<br>Introduction<br>The AirPure Elite 7000 is a state-of-the-art air purifier engineered to<br>provide superior indoor air quality. Designed with modern households<br>in mind, this appliance ensures a clean and healthy living<br>environment by removing 99.97% of airborne particles, including<br>irritants and pollutants.<br>Specifications<br>Room Coverage:  Up to 700 square feet<br>Filtration System:  True HEPA filter coupled with an activated<br>carbon filter<br>Noise Level:  25-55 dB, with night mode for ultra-quiet<br>operation<br>Dimensions:  24" x 14" x 8"<br>Weight:  18 pounds<br>Smart Compatibility:  Alexa, Google Home<br>Power:  110-240V, 50-60Hz<br>Setup Guide<br>Unboxing:  Carefully remove the AirPure Elite 7000 and all its<br>components from the packaging.<br>Filter Installation:  Open the back panel, remove the packing<br>material from the filters, and secure them.<br>Placement:  Position the unit in the center of the room for<br>optimal air distribution.• <br>• <br>• <br>• <br>• <br>• <br>• <br>1. <br>2. <br>3. <br>Power On:  Plug the device into a power outlet and press the<br>power button located on the top panel.<br>Smart Connection:  Download the AirPure app, follow the<br>prompts to connect to your Wi-Fi, and pair with Alexa or Google<br>Home.<br>Usage Instructions<br>Mode Selection:  Use the touch panel or app to choose from<br>Auto, Manual, or Sleep mode.<br>Air Quality Sensor:  Monitors air quality in real time,<br>automatically adjusting settings for optimal performance.<br>Timer Function:  Set operation time in 1-hour increments up to<br>12 hours.<br>Maintenance and Care<br>Filter Replacement:  Replace the HEPA filter every 6 months;<br>the activated carbon filter every 12 months or as needed.<br>Cleaning:  Wipe down the exterior weekly with a damp cloth;<br>vacuum the air intake grille monthly.<br>Error Indicators:  The unit will alert you with indicator lights if<br>maintenance is needed.<br>Integration Guides<br>Alexa Setup:  Open the Alexa app, navigate to Devices, and add<br>the AirPure Elite 7000. Voice examples include, "Alexa, turn on<br>AirPure."<br>Google Home Setup:  Add through Devices in the Google Home<br>app, and use commands like "Hey Google, adjust AirPure to<br>sleep mode."4. <br>5. <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>Safety Warnings and Compliance<br>Child Lock Feature:  Activate the child lock from the control<br>panel or mobile app to prevent accidental operation.<br>Compliance:  Certified with UL and CARB standards for electrical<br>and air quality safety.<br>Troubleshooting<br>Device Won't Turn On:  Check power connection, try a different<br>outlet, hold the power button for 10 seconds to reset.<br>Weak Airflow:  Inspect and clean the filter; check the intake and<br>outflow for obstruction.<br>Warranty and Customer Support<br>Warranty:  2-year manufacturer warranty covering defects and<br>malfunctions.<br>Contact:  For support, reach out at support@brownbox.com or<br>call 1-800-667-2345. Available 7 days a week.<br>Thank you for choosing the AirPure Elite 7000. Your health and<br>comfort are our top priorities. Enjoy the purity of quality air.• <br>• <br>• <br>• <br>• <br>•|Home Appliances|Air Purifier|<product_category>Home Appliances</product_category><br><product_sub_category>Air Purifier</product_sub_category><br><product_name>AirPure Elite 7000</product_name><br><product_doc><br>AirPure Elite 7000 Product<br>Documentation<br>Introduction<br>The AirPure Elite 7000 is a state-of-the-art air purifier engineered to<br>provide superior indoor air quality. Designed with modern households<br>in mind, this appliance ensures a clean and healthy living<br>environment by removing 99.97% of airborne particles, including<br>irritants and pollutants.<br>Specifications<br>Room Coverage:  Up to 700 square feet<br>Filtration System:  True HEPA filter coupled with an activated<br>carbon filter<br>Noise Level:  25-55 dB, with night mode for ultra-quiet<br>operation<br>Dimensions:  24" x 14" x 8"<br>Weight:  18 pounds<br>Smart Compatibility:  Alexa, Google Home<br>Power:  110-240V, 50-60Hz<br>Setup Guide<br>Unboxing:  Carefully remove the AirPure Elite 7000 and all its<br>components from the packaging.<br>Filter Installation:  Open the back panel, remove the packing<br>material from the filters, and secure them.<br>Placement:  Position the unit in the center of the room for<br>optimal air distribution.• <br>• <br>• <br>• <br>• <br>• <br>• <br>1. <br>2. <br>3. <br>Power On:  Plug the device into a power outlet and press the<br>power button located on the top panel.<br>Smart Connection:  Download the AirPure app, follow the<br>prompts to connect to your Wi-Fi, and pair with Alexa or Google<br>Home.<br>Usage Instructions<br>Mode Selection:  Use the touch panel or app to choose from<br>Auto, Manual, or Sleep mode.<br>Air Quality Sensor:  Monitors air quality in real time,<br>automatically adjusting settings for optimal performance.<br>Timer Function:  Set operation time in 1-hour increments up to<br>12 hours.<br>Maintenance and Care<br>Filter Replacement:  Replace the HEPA filter every 6 months;<br>the activated carbon filter every 12 months or as needed.<br>Cleaning:  Wipe down the exterior weekly with a damp cloth;<br>vacuum the air intake grille monthly.<br>Error Indicators:  The unit will alert you with indicator lights if<br>maintenance is needed.<br>Integration Guides<br>Alexa Setup:  Open the Alexa app, navigate to Devices, and add<br>the AirPure Elite 7000. Voice examples include, "Alexa, turn on<br>AirPure."<br>Google Home Setup:  Add through Devices in the Google Home<br>app, and use commands like "Hey Google, adjust AirPure to<br>sleep mode."4. <br>5. <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>• <br>Safety Warnings and Compliance<br>Child Lock Feature:  Activate the child lock from the control<br>panel or mobile app to prevent accidental operation.<br>Compliance:  Certified with UL and CARB standards for electrical<br>and air quality safety.<br>Troubleshooting<br>Device Won't Turn On:  Check power connection, try a different<br>outlet, hold the power button for 10 seconds to reset.<br>Weak Airflow:  Inspect and clean the filter; check the intake and<br>outflow for obstruction.<br>Warranty and Customer Support<br>Warranty:  2-year manufacturer warranty covering defects and<br>malfunctions.<br>Contact:  For support, reach out at support@brownbox.com or<br>call 1-800-667-2345. Available 7 days a week.<br>Thank you for choosing the AirPure Elite 7000. Your health and<br>comfort are our top priorities. Enjoy the purity of quality air.• <br>• <br>• <br>• <br>• <br>•<br></product_doc>|

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✓ **Pipeline Complete!**
# MAGIC
# MAGIC We have successfully:
# MAGIC 1. Loaded products and product documentation from Unity Catalog
# MAGIC 2. Joined the tables on `product_name`
# MAGIC 3. Created indexed documents with XML-style formatting
# MAGIC 4. Saved the results to `agentic_catalog.agentic_schema.products_indexed`
# MAGIC
# MAGIC The indexed documents are now ready for:
# MAGIC - Vector embeddings generation
# MAGIC - LLM-based retrieval
# MAGIC - Semantic search applications
# MAGIC - AI agent workflows

# COMMAND ----------

spark.sql("""
ALTER TABLE agentic_catalog.agentic_schema.product_docs_combined
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE 
# MAGIC agentic_catalog.agentic_schema.product_docs_combined SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days')
