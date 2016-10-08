"""
The datastore script consolidates methods to connect to the CMDB traditional data store.
"""

import logging
import pymysql
import sys
from collections import namedtuple


class DataStore:

    def __init__(self, config):
        """
        Datastore initialization. Connection to the database is configured. A cursor is created.
        :param config: Ini file object.
        :return:
        """
        # Get MySQL Connection
        db_config = {
            'user': config['Database']['username'],
            'password': config['Database']['password'],
            'host': config['Database']['host'],
            'database': config['Database']['database'],
        }
        try:
            self.cnx = pymysql.connect(**db_config)
        except:
            logging.exception("Something went wrong: ")
            sys.exit(1)
        self.cursor = self.cnx.cursor()

    def get_named_row(self):
        """
        This method will create a named tuple row for the current cursor.
        :return: namedtuple class with name "named_row"
        """
        # Get column names
        field_list = [x[0] for x in self.cursor.description]
        # Create named tuple subclass with name "named_row"
        named_row = namedtuple("my_named_row", field_list, rename=True)
        return named_row

    def get_ci_type_translation(self):
        """
        Thie method will get ci_type translation table. Returns dictionary with ci_type to component translations.
        :return: dictionary ci_type - translation
        """
        ci_type = {}
        query = "SELECT * FROM comp_type"
        self.cursor.execute(query)
        named_row = self.get_named_row()
        # rows = self.cursor.fetchall()
        for row in map(named_row._make, self.cursor.fetchall()):
            ci_type[row.CI_TYPE] = row.comp_type
        return ci_type

    def get_components(self):
        """
        This method will return all components with all attributes from the CMDB store.
        :return: Array of components. Each component is a dictionary in the array.
        """
        row_list = []
        query = "SELECT * FROM dc_component"
        self.cursor.execute(query)
        named_row = self.get_named_row()
        for row in map(named_row._make, self.cursor.fetchall()):
            row_list.append(row._asdict())
        return row_list

    def get_relations(self):
        """
        This method will return all relations from the CMDB store. CMDB ID from source and target are returned, together
        with relation type.
        :return: Source and target ID for the translation, with relation type.
        """
        row_list = []
        query = "SELECT cmdb_id_source, relation, cmdb_id_target " \
                "FROM dc_relations WHERE not " \
                "relation = 'maakt gebruik van'"
        self.cursor.execute(query)
        named_row = self.get_named_row()
        for row in map(named_row._make, self.cursor.fetchall()):
            row_list.append(row._asdict())
        return row_list
