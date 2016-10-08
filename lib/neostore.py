"""
This class consolidates functions related to the neo4J datastore.
"""

import logging
import sys
from py2neo import Graph, Node, Relationship
from py2neo.ext.calendar import GregorianCalendar
from py2neo.database import DBMS


class NeoStore:

    def __init__(self, config, refresh='No'):
        """
        Method to instantiate the class in an object for the neostore module.
        :param config object, to get connection parameters.
        :param refresh: If Yes, then database will be made empty.
        :return: Object to handle neostore commands.
        """
        logging.debug("Initializing Neostore object")
        self.config = config
        self.graph = self._connect2db()
        if refresh == 'Yes':
            self._delete_all()
        self.calendar = GregorianCalendar(self.graph)
        return

    def _connect2db(self):
        """
        Internal method to create a database connection. This method is called during object initialization.
        :return: Database handle and cursor for the database.
        """
        logging.debug("Creating Neostore object.")
        neo4j_config = {
            'user': self.config['Graph']['username'],
            'password': self.config['Graph']['password'],
        }
        # Connect to Graph
        graph = Graph(**neo4j_config)
        # Check that we are connected to the expected Neo4J Store - to avoid accidents...
        dbname = DBMS().database_name
        if dbname != self.config['Graph']['neo_db']:
            logging.fatal("Connected to Neo4J database {d}, but expected to be connected to {n}"
                          .format(d=dbname, n=self.config['Main']['neo_db']))
            sys.exit(1)
        return graph

    def create_node(self, *labels, **props):
        """
        Function to create node. The function will return the node object.
        @param labels: Labels for the node
        @param props: Value dictionary with values for the node.
        @return:
        """
        component = Node(*labels, **props)
        self.graph.create(component)
        return component

    def create_relation(self, left_node, rel, right_node):
        """
        Function to create relationship between nodes.
        @param left_node:
        @param rel:
        @param right_node:
        @return:
        """
        rel = Relationship(left_node, rel, right_node)
        self.graph.merge(rel)
        return

    def _delete_all(self):
        """
        Function to remove all nodes and relations from the graph database.
        Then create calendar object.
        :return:
        """
        logging.info("Remove all nodes and relations from database.")
        self.graph.delete_all()
        return

    def link2date(self, component, rel, y, m, d):
        """
        This function will link the component to a date
        :param component: Node of the component to link to
        :param rel: Type of relation to link to
        :param y: Year, in int
        :param m: Month, in int
        :param d: Day, in int
        :return:
        """
        date_node = self.calendar.date(y, m, d).day
        self.create_relation(component, rel, date_node)
        return
