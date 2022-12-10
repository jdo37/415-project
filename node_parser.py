from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import time
from datetime import datetime

class Node:
    def __init__(self, id, uploader, age, category, length, views, rate, ratings, comments, related):
        self.id = id
        self.uploader = uploader
        self.age = int(age)
        self.category = category
        self.length = int(length)
        self.views = int(views)
        self.rate = float(rate)
        self.ratings = int(ratings)
        self.comments = int(comments)
        self.related = related


class NodeParser:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()

    #creates video node
    def create_node(self, node):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._create_and_return_node, node)

    @staticmethod
    def _create_and_return_node(tx, node):
        query = (
            "CREATE (v1:Video { id: $id, age: $age, category: $category, length: $length, views: $views, rate: $rate, ratings: $ratings, comments: $comments }) "
            "RETURN v1"
        )
        result = tx.run(query, id=node.id, age=node.age, category=node.category, length=node.length, views=node.views, rate=node.rate, ratings=node.ratings, comments=node.comments)
        try:
            return [{"v1": row["v1"]["id"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise
    
    #creates relationship between videos
    def create_relationship(self, node, related):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._create_and_return_relationship, node, related)

    @staticmethod
    def _create_and_return_relationship(tx, node, related):
        query = (
            "MATCH (v1:Video), (v2:Video) "
            "WHERE v1.id = $id1 AND v2.id = $id2 "
            "CREATE (v1)-[r:RELATEDTO]->(v2) "
            "RETURN type(r)"
        )
        result = tx.run(query, id1=node.id, id2=related)

    #creates node for category
    def create_category_node(self, category):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._create_and_return_category_node, category)

    @staticmethod
    def _create_and_return_category_node(tx, category):
        query = (
            "CREATE (c1:Category { name: $name }) "
            "RETURN c1"
        )
        result = tx.run(query, name=category)
        try:
            return [{"c1": row["c1"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    #create relationship between category and video
    def create_category_relationship(self, node):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._create_and_return_category_relationship, node)

    @staticmethod
    def _create_and_return_category_relationship(tx, node):
        query = (
            "MATCH (v1:Video), (c1:Category) "
            "WHERE v1.id = $id AND c1.name = $name "
            "CREATE (c1)-[r:CATEGORYOF]->(v1) "
            "RETURN type(r)"
        )
        result = tx.run(query, id=node.id, name=node.category)

def create_nodes(nodes, parser):
    for node in nodes:
        parser.create_node(node)

def create_relationships(nodes, parser):
    for node in nodes:
        for related in node.related:
            parser.create_relationship(node, related)

def create_categories(categories, parser):
    for category in categories:
        parser.create_category_node(category)

def create_category_rel(nodes, parser):
    for node in nodes:
        parser.create_category_relationship(node)

nodes = []
files = ['3-0.txt', '3-1.txt', '3-2.txt']
categories = []
for file in files:
    with open(file) as f:
        for line in f:
            info = line.split('\t')
            if len(info) > 1:
                related = info[9:]
                node = Node(info[0], info[1], info[2], info[3], info[4], info[5], info[6], info[7], info[8], related)
                nodes.append(node)
                if info[3] not in categories:
                    categories.append(info[3])
            

if __name__ =="__main__":
    uri = "bolt://localhost:7687"
    user = "neo4j"
    #password for neo4j database
    password = ""
    print(datetime.now().strftime("%H:%M:%S"))
    parser = NodeParser(uri, user, password)
    create_nodes(nodes, parser)
    create_relationships(nodes, parser)
    create_categories(categories, parser)
    create_category_rel(nodes, parser)
    print(datetime.now().strftime("%H:%M:%S"))
    parser.close()