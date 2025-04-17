# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import os
import random
import json
import jmespath
import networkx as nx
from matplotlib.patches import Patch
import matplotlib.pyplot as plt
import logging

class SayariGraphScrapingPipeline:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.root_dir = os.path.dirname(os.path.dirname(__file__))
        self.output_dir = os.path.join(
            self.root_dir,
            "output"
        )
        os.makedirs(self.output_dir, exist_ok=True)
        self.graph_path = os.path.join(self.output_dir, "graph.jsonl")
        self.knowledge_graph = []
        self.nodes = set()

    def open_spider(self, spider):
        return
    
    def process_item(self, item, spider):
        self.write_to_knowledge_graph(item) # Add to knowledge graph 
        return item

    def write_to_knowledge_graph(self, item):
        is_relation_found = False
        company_title = jmespath.search("TITLE[0]", item)
        if company_title is None:
            self.log_warn_msg("Company title not found", item)

        self.nodes.add(company_title)
        if "DRAWER_DETAIL_LIST" in item:
            interested_labels = ("COMMERCIAL_REGISTERED_AGENT", "REGISTERED_AGENT", "OWNER_NAME")
            for detail in item["DRAWER_DETAIL_LIST"]:
                label_name = detail.get("LABEL", None)
                if not isinstance(label_name, str):
                    self.log_warn_msg(f"Label {label_name} not string", item)

                label_name = SayariGraphScrapingPipeline.normalize_str(label_name) # bring to consistent format for extraction
                if label_name and label_name in interested_labels:
                    row_to_write = (detail.get("VALUE", None), company_title, {"label" : label_name})
                    self.knowledge_graph.append(row_to_write)
                    is_relation_found = True
            
            if is_relation_found is False:
                self.log_warn_msg(f"Expected graph label names not found", item)
        else:
            self.log_warn_msg("DRAWER_DETAIL_LIST was not found", item)

    def draw_and_save_knowledge_graph(self):
        G = nx.Graph()
        G.add_nodes_from(self.nodes)
        G.add_edges_from(self.knowledge_graph) # Populate Graph

        # Assign colors
        # label_color_map = {
            # 'COMMERCIAL_REGISTERED_AGENT': 'teal',
            # 'REGISTERED_AGENT': 'lightblue',
            # 'OWNER_NAME': 'lightgreen'
        # }
        # Assign color to each edge based on its label
        # for _, _, d in G.edges(data=True):
            # d['color'] = label_color_map.get(d['label'], 'black')  # default to black if label not found

        # Set up standard spring layout network graph
        pos = nx.nx_agraph.pygraphviz_layout(G) # add some distance
        fig, ax = plt.subplots(figsize=(10,6))
        C = (G.subgraph(c) for c in nx.connected_components(G))
        for g in C:
            c = [random.random()] * nx.number_of_nodes(g)  # random color...
            nx.draw(g, pos, ax=ax, node_size=40, node_color=c, vmin=0.0, vmax=1.0)
        
        
        
        # nx.draw_networkx_nodes(G, pos, ax=ax, node_size=20)
        # nx.draw_networkx_labels(G, pos)

        # edge_colors = [d['color'] for _, _, d in G.edges(data=True)]
        # edge_labels = {(u, v): d['label'] for u, v, d in G.edges(data=True)}

        # nx.draw_networkx_edges(G, pos, ax=ax, arrows=False,
        #                        width=1)
        # nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color='black')

        # legend_handles = [
        #     Patch(color=color, label=label) for label, color in label_color_map.items()
        # ]
        # Display
        # plt.figure(dpi=200)
        plt.axis('off')
        # plt.legend(handles=legend_handles, title="Edge Labels", loc="lower left",
        #            bbox_to_anchor=(0,0), bbox_transform=fig.transFigure,
        #            ncols=3)
        plt.title("North Dakota Web App Business Relationships", x=-0.1, y=1, va="bottom", ha="left", 
                  fontsize=20, fontweight=800)
        # plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "knowledge_graph.png"))
    
    @staticmethod
    def normalize_str(s):
        '''
            upper case, and replace contiguous spaces with _
        '''
        words = str(s).upper().split(" ")
        return '_'.join([word for word in words if word != ""])    
    
    def log_warn_msg(self, warn_msg, item):
        business_id = item.get("ID") or item.get("KEY_ID")
        final_msg = warn_msg + " " + f"for business id: {business_id}"
        self.logger.warning(final_msg)
    
    def close_spider(self, spider):
        if self.knowledge_graph:
            # Write out stored data and plots
            # If data gets large we can also change our implementation to
            # process writes in batches. 
            with open(self.graph_path, mode="w") as f:
                for line in self.knowledge_graph:
                    f.write(json.dumps(line) + "\n")

            self.draw_and_save_knowledge_graph()
    
    
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
#             row_to_write = (item["TITLE"][0], item["COMMERCIAL_REGISTERED_AGENT"], "COMMERCIAL_REGISTERED_AGENT")
#             self.knowledge_graph.append(row_to_write)
#         if "REGISTERED_AGENT" in item:
#             row_to_write = (item["TITLE"][0], item["REGISTERED_AGENT"], "REGISTERED_AGENT")
#             self.knowledge_graph.append(row_to_write)
#         if "OWNER_NAME" in item:
#             row_to_write = (item["TITLE"][0], item["OWNER_NAME"], "OWNER")
#             self.knowledge_graph.append(row_to_write)

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
