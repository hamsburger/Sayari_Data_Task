# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter
import os
import random
import json
import jmespath
import networkx as nx
import matplotlib.pyplot as plt
import logging
import re
import polars as pl


class SayariGraphScrapingPipeline:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.root_dir = os.path.dirname(os.path.dirname(__file__))
        self.output_dir = os.path.join(self.root_dir, "output")
        os.makedirs(self.output_dir, exist_ok=True)
        self.graph_path = os.path.join(self.output_dir, "graph.csv")
        self.knowledge_graph = []  # Store edges
        # self.nodes = set()  # Store nodes

    def open_spider(self, spider):
        return

    def process_item(self, item, spider):
        self.write_to_knowledge_graph(item)  # Add to knowledge graph
        return item

    def write_to_knowledge_graph(self, item):
        is_relation_found = False
        # interesting relationships to capture with company
        interested_labels = (
            "COMMERCIAL_REGISTERED_AGENT",
            "REGISTERED_AGENT",
            "OWNER_NAME",
            "OWNERS",
        )
        company_title = jmespath.search("TITLE[0]", item)
        if company_title is None:
            self.log_warn_msg("Company title not found", item)
        company_title = company_title.upper().strip()

        # If company title does not start with x, don't incorporate into knowledge graph
        if company_title and not company_title.startswith("X"):
            return

        # self.nodes.add(company_title)
        if "DRAWER_DETAIL_LIST" in item:
            details = item["DRAWER_DETAIL_LIST"]
            i = 0
            # Go through drawer to extract all drawer labels and values
            while i < len(details):
                detail = details[i]
                label_name = detail.get("LABEL", None)
                self.check_string_warning(label_name, f"Label not string", item)

                # bring label name to consistent format for extraction
                label_name = SayariGraphScrapingPipeline.normalize_label_str(label_name)
                if label_name in interested_labels:
                    if label_name == "OWNERS":
                        # Write the current owner
                        edge_to_write = (
                            self.reformat_and_check_data_for_knowledge_graph(
                                detail.get("VALUE", None),
                                company_title,
                                label_name,
                                item,
                            )
                        )
                        self.knowledge_graph.append(edge_to_write)
                        i += 1
                        # Write all succeeding relationships to knowledge graph,
                        # indicated by label_name == ""
                        while i < len(details):
                            detail = details[i]
                            if label_name == "":
                                self.reformat_and_check_data_for_knowledge_graph(
                                    detail.get("VALUE", None),
                                    company_title,
                                    label_name,
                                    item,
                                )
                                self.knowledge_graph.append(edge_to_write)
                                i += 1
                            else:
                                # No more owners to extract, break
                                break
                    
                    else:
                        value = detail.get("VALUE", None)
                        edge_to_write = (
                            self.reformat_and_check_data_for_knowledge_graph(
                                value, company_title, label_name, item
                            )
                        )
                        self.knowledge_graph.append(edge_to_write)
                        i += 1
                    is_relation_found = True
                else:
                    i += 1
            if is_relation_found is False:
                self.log_warn_msg(f"Expected graph label names not found", item)
        else:
            self.log_warn_msg("DRAWER_DETAIL_LIST was not found", item)

    def reformat_and_check_data_for_knowledge_graph(
        self, value, company_title, label_name, item
    ):  
        company_title = re.sub(r"\s+", " ", company_title)
        if not self.check_string_warning(
            value, f"Value for {label_name} not string", item
        ):
            value = value.split("\n")[0].upper().strip() # Store only string before \n
                                                         # For strings without \n, just stores
                                                         # entire string
            value = re.sub(r"\s+", " " , value)

        edge_to_write = [
            value,
            company_title,
            {"label": "OWNER_NAME" if label_name == "OWNERS" else label_name},
        ]
        return edge_to_write

    def draw_and_save_knowledge_graph(self):
        G = nx.Graph()
        # G.add_nodes_from(self.nodes)
        G.add_edges_from(self.knowledge_graph)  # Populate Graph

        pos = nx.nx_agraph.pygraphviz_layout(G, prog="neato")  # add some distance
        fig, ax = plt.subplots(figsize=(10, 6), dpi=200)
        C = (G.subgraph(c) for c in nx.connected_components(G))
        for g in C:
            c = [random.random()] * nx.number_of_nodes(g)  # random color...
            nx.draw(g, pos, ax=ax, node_size=20, node_color=c, vmin=0.0, vmax=1.0)
        plt.axis("off")
        plt.title(
            "North Dakota Web App Business Relationships",
            x=0,
            y=1,
            va="bottom",
            ha="left",
            fontsize=20,
            fontweight=800,
        )
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "knowledge_graph.png"))

    @staticmethod
    def normalize_label_str(s):
        """
        Upper case and strip string, and replace contiguous spaces with _
        """
        words = str(s).upper().split(" ")
        return "_".join([word for word in words if word != ""])

    def check_string_warning(self, the_string, warn_msg, item):
        """
        Send warning when expected string is not string
        """
        if not isinstance(the_string, str):
            self.log_warn_msg(f"{warn_msg} : {the_string}", item)
            return True
        else:
            return False

    def log_warn_msg(self, warn_msg, item):
        """
        Warning messages for debugging
        potential errors, but not severe enough
        to halt web crawling process.
        """
        business_id = item.get("ID") or item.get("KEY_ID")
        control_id = item.get("RECORD_NUM")
        final_msg = (
            warn_msg
            + " "
            + f"for business id: {business_id} and SOS Control ID#: {control_id}"
        )
        self.logger.warning(final_msg)

    def close_spider(self, spider):
        """
        Write out stored data and plots
        If data gets large we can also change our implementation to
        process writes in batches.
        """
        if self.knowledge_graph:
            # Write out graph in csv format for reading and
            # bulk loading into structured databases (ex. Postgres)
            from_node = jmespath.search("[*][0]", self.knowledge_graph)
            to_node = jmespath.search("[*][1]", self.knowledge_graph)
            relationship_type = jmespath.search("[*][2].label", self.knowledge_graph)
            df = pl.DataFrame(
                {"entity": from_node, "company": to_node, "relationship": relationship_type}
            )
            df.write_csv(self.graph_path)

            # print("Number of Companies Plotted:", len(self.nodes))
            print("Nubmer of Relationships Plotted:", len(self.knowledge_graph))
            self.draw_and_save_knowledge_graph()
