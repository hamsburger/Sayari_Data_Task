# class PostgresScrapingPipeline:
#     def __init__(self):
#         self.knowledge_graph = []
#         self.batch_size = 500

#     def open_spider(self, spider):
#         self.conn = psycopg2.connect(
#             dbname=os.getenv("DBNAME"),
#             user=os.getenv("POSTGRES_USERNAME"),
#             password=os.getenv("PASSWORD"),
#             host=os.getenv("POSTGRES_HOST"),
#             port=os.getenv("POSTGRES_PORT") # 5432 typical
#         )
#         self.sql_directory = os.path.join(os.path.dirname(__file__), "sql")
#         self.conn.autocommit = True
#         self.cursor = self.conn.cursor()
#         with open(os.path.join(self.sql_directory, "insert_relationship.sql"), mode="r") as f:
#             self.insert_sql = f.read()
        
#         with open(os.path.join(self.sql_directory, "create_business_graph_table.sql"), mode="r") as f:
#             self.graph_table_sql = f.read()

#         self.create_business_graph_table()
    

#     def create_business_graph_table(self):
#         self.cursor.execute(self.graph_table_sql)

#     def add_to_buffer(self, item):
#         '''
#             Build data format to analyze relationships between companies.
#         '''
#         if "COMMERCIAL_REGISTERED_AGENT" in item:
#             edge_to_write = (item["TITLE"][0], item["COMMERCIAL_REGISTERED_AGENT"], "COMMERCIAL_REGISTERED_AGENT")
#             self.knowledge_graph.append(edge_to_write)
#         if "REGISTERED_AGENT" in item:
#             edge_to_write = (item["TITLE"][0], item["REGISTERED_AGENT"], "REGISTERED_AGENT")
#             self.knowledge_graph.append(edge_to_write)
#         if "OWNER_NAME" in item:
#             edge_to_write = (item["TITLE"][0], item["OWNER_NAME"], "OWNER")
#             self.knowledge_graph.append(edge_to_write)

#     def process_item(self, item, spider):
#         self.add_to_buffer(item)
        
#         if len(self.knowledge_graph) >= self.batch_size:
#             self.cursor.executemany(self.insert_row_sql, self.knowledge_graph)
#             self.knowledge_graph.clear()

#         return item

#     def close_spider(self, spider):
#         if self.knowledge_graph:
#             self.cursor.executemany(self.insert_row_sql, self.knowledge_graph)
#             self.knowledge_graph.clear()

#         # Craft our visualization here

#         self.cursor.close()
#         self.conn.close()
