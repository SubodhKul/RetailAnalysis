from lib import configReader

# Defining customers schema
def get_customers_schema():
    schema = "customer_id int, customer_fname string, customer_lname string, username string, password string, address string, city string, state string, pincode string"
    return schema

# Creating customers DataFrame
def read_customers(spark, env):
    conf = configReader.get_app_config(env)
    customers_file_path = conf["customers.file.path"]
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_customers_schema()) \
        .load(customers_file_path)

# Defining orders schema
def get_orders_schema():
    schema = "order_id int, order_date string, customer_id int, order_status string"
    return schema

# Creating orders DataFrame
def read_orders(spark, env):
    conf = configReader.get_app_config(env)
    orders_file_path = conf["orders.file.path"]
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_orders_schema()) \
        .load(orders_file_path)

