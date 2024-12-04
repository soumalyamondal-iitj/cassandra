
from astrapy import DataAPIClient
import csv
class CassandraDB():
    def __init__(self):
        self.db = None
        self.keyspace = 'n_customer'  # Replace with your keyspace name
        self.collection_name = 'customer'
        self.token = 'AstraCS:mwiyjHsLMAcXMgjDiEjfBcnb:b97d09c81075396cf0fbe4f3b24fb1939f34a54ec60bcfb1e4db7ca7d30ec75a'  # Replace with your application token
        self.database_id = '1e872ab8-74f5-4c36-ad27-4f42d62ba9bc'  # Replace with your database ID
        self.database_region = 'us-east1'  # Replace with your database region

    def connect(self):
        # Initialize the client
        client = DataAPIClient(self.token)
        self.db = client.get_database_by_api_endpoint(
          "https://1e872ab8-74f5-4c36-ad27-4f42d62ba9bc-us-east1.apps.astra.datastax.com",
            namespace="n_customer",
        )
        print(f"Connected to Astra DB: {self.db.list_collection_names()}")

    def create_table(self):
        # In Astra DB, collections (tables) are created implicitly when data is inserted
        self.db.create_collection(self.collection_name)
        print("Table 'Customer' created successfully")
       

    def load_data(self):
        # Load customer.csv data into the "Customer" collection
        with open('C:\Data\Assig-7\data\customers.csv', 'r') as file:  # Replace with the path to your customers.csv
            reader = csv.DictReader(file)
            for row in reader:
                customer_data = {
                    'id': int(row['id']),
                    'gender': row['gender'],
                    'age': int(row['age']),
                    'number_of_kids': int(row['number_of_kids'])
                }
                self.db.get_collection(self.collection_name).insert_one(customer_data)
        print("Data loaded into 'Customer' table")

    def query_1(self):
        # Query 1: Return the age of the customer whose id is 979863
        result = self.db.get_collection(self.collection_name).find_one({'id': 979863})
        if result:
            print(f"Age of customer with id 979863: {result['age']}")
        else:
            print("Customer with id 979863 not found")
        return result

    def query_2(self):
        # Query 2: Return information of customers who are “MALE” and age is 25 or 35
        query = {'gender': 'MALE', 'age': {'$in': [25, 35]}}
        results = self.db.get_collection(self.collection_name).find(query)
        customers = []
        print("Customers who are “MALE” and age is 25 or 35 :")
        for row in results:
            customer_info = {
                'id': row['id'],
                'gender': row['gender'],
                'age': row['age'],
                'number_of_kids': row['number_of_kids']
            }
            customers.append(customer_info)
            print(customer_info)
        return customers

if __name__ == '__main__':
    client = CassandraDB()
    client.connect()
    #client.create_table()
    #client.load_data()
    client.query_1()
    client.query_2()