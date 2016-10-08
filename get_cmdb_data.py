"""
This script will collect all cmdb data from MySQL database and load it in Neo4J database.
"""

import logging
import sys

from lib import my_env
from lib.datastore import DataStore
from lib.neostore import NeoStore

loc_nodes = {}
sw_prod_nodes = {}
product_nodes = {}
producent_nodes = {}
product_component_types = ['Computer', 'Component', 'Storage']


def get_component_type(f_row, f_ci_type):
    """
    This function gets all attributes from a node and calculates the Component Type.
    Component type is calculated from ci_class if available. If not available then ci_type translation is used from
    ci_translation table.
    ci_class attribute is removed from row of attributes.
    :param f_row:
    :param f_ci_type: list with ci_type values that make a Toepassingcomponentinstallatie.
    :return: Component Type (string) and component attributes (list) minus ci_type, ci_categorie, ci_class
    """
    # if ci_class is available, then use this value
    if f_row['CI_CLASS']:
        comp_type = f_row['CI_CLASS']
        if comp_type == 'toepassingcomponentinstallatie':
            comp_type = 'toepassingomgeving'
        del f_row["CI_CLASS"]
    else:
        comp_type = f_ci_type[f_row['CI_TYPE']]
    return comp_type, f_row


def get_location(f_row):
    """
    This method will check if a location is defined for the object. If so:
    1. Check if location node exists
    2. Create location node if it does not exist
    3. Return location node or empty string
    :param f_row:
    :return: location node or empty string and attribute collection less location node.
    """
    if f_row['LOCATIE']:
        # Remember location, add when node is created
        location = f_row['LOCATIE']
        try:
            # Try if Node for the location has been created, re-use it.
            loc_node = loc_nodes[location]
        except KeyError:
            # Location exist, but node needs to be created
            loc_node = ns.create_node('Location', name=location)
            # Remember node for next usage
            loc_nodes[location] = loc_node
            # Show me the type
    else:
        # Location does not exist, return empty object
        loc_node = ''
    # Location has been handled, remove it
    del f_row['LOCATIE']
    return loc_node, f_row


def link_eol(f_component, f_uitdovend, f_uitgedoofd):
    """
    This method will link the component to the uitdovend / uitgedoofd End-Of-System-Life dates, if available.
    :param f_component: Component node that is investigated.
    :param f_uitdovend: Uitdovend Date (Format: DD-MM-YYYY)
    :param f_uitgedoofd: Uitgedoofd Date (Format: DD-MM-YYYY)
    :return: nothing
    """
    if len(f_uitdovend) > 4:
        # Uitdovend Date is defined, link to component
        dd, mm, yyyy = f_uitdovend.split("-")
        ns.link2date(f_component, "Uitdovend", int(yyyy), int(mm), int(dd))
    if len(f_uitgedoofd) > 4:
        # Uitdovend Date is defined, link to component
        dd, mm, yyyy = f_uitgedoofd.split("-")
        ns.link2date(f_component, "Uitgedoofd", int(yyyy), int(mm), int(dd))
    return


def create_product(f_row):
    """
    This method will create product node.
    Check if producent already exists. If not, then create Producent.
    Attach Product to Producent.
    :param f_row:
    :return: product node
    """
    producent_id = f_row['PRODUCENT']
    try:
        producent_node = producent_nodes[producent_id]
    except KeyError:
        # Create Producent Node
        producent_node = ns.create_node("Producent", name=producent_id)
        # Remember producent_node for next iterations
        producent_nodes[producent_id] = producent_node
    # Create Product Node
    f_product_node = ns.create_node("Product", name=f_row['PRODUCT'])
    ns.create_relation(producent_node, "Product", f_product_node)
    return f_product_node


def get_product_node(f_row):
    """
    This method will check if producent and product are defined for the component. If so, then the producent / product
    are converted into a node. The product node is returned.
    The producent / product items are removed from the row. For component types in scope, there is no versie defined.
    :param f_row:
    :return: product node, attribute row without producent and product.
    """
    product_id = "{0}*{1}".format(str(f_row['PRODUCENT']), str(f_row['PRODUCT']))
    if len(product_id) == 1:
        f_product_node = "No Product Defined"
    else:
        try:
            f_product_node = product_nodes[product_id]
        except KeyError:
            # Product Node does not exist, create it
            f_product_node = create_product(f_row)
            # And remember Product for next iterations
            product_nodes[product_id] = f_product_node
    del f_row['PRODUCENT']
    del f_row['PRODUCT']
    return f_product_node, f_row


def create_sw_product(f_row):
    """
    This method will create a SW Product Node for SW Product (Producent - Product - Versie).
    Check if Product exists. If so, attach SW Versie to Product. Else create Product.
    :param f_row:
    :return:
    """
    f_product_node, f_row = get_product_node(f_row)
    f_valuedict = {'Categorie': f_row['CI_CATEGORIE'],
                   'Versie': f_row['VERSIE']}
    # Create SW Product - Version
    f_component = ns.create_node(node_label, **f_valuedict)
    # Link to SW Product Component
    ns.create_relation(f_product_node, 'Versie', f_component)
    # Link to EOL identifiers
    link_eol(f_component, f_row['UITDOVEND_DATUM'], f_row['UITGEDOOFD_DATUM'])
    return f_component


def handle_sw_product(f_row):
    """
    For SW_Prod_Install extract Product details and create nodetree for producent - product - versie
    Then remember versie node to link with every Component.
    Purpose: Producent-Product-Versie is unique, 'INSTALLED ON' is the relationship that is required.
    :param f_row: Attribute collection
    :return: Node component for SW component
    """
    # Producent, Product are version are never NULL. Use this information.
    sw_product = "{0}*{1}*{2}".format(str(f_row['PRODUCENT']), str(f_row['PRODUCT']), str(f_row['VERSIE']))
    try:
        sw_prod_node = sw_prod_nodes[sw_product]
    except KeyError:
        # SW Product Node does not exist, create one
        sw_prod_node = create_sw_product(f_row)
        # And remember SW Product for following iterations
        sw_prod_nodes[sw_product] = sw_prod_node
    return sw_prod_node


if __name__ == "__main__":
    cfg = my_env.init_env("cmdb", __file__)
    # Get Neo4J Connetion and clean Database
    ns = NeoStore(cfg, refresh="Yes")
    # Get DataStore object
    ds = DataStore(cfg)
    # Initialize ci_type translation
    ci_type = ds.get_ci_type_translation()
    # Then get all Component rows
    rows = ds.get_components()
    node_obj = {}
    loc_obj = {}
    node_info = my_env.LoopInfo("Nodes", 100)
    for row in rows:
        del row["ID"]
        node_label, row = get_component_type(row, ci_type)
        if node_label == 'SW_Prod_Install':
            component = handle_sw_product(row)
        else:
            # Not a SW Product, so default component handling
            on_location, row = get_location(row)
            # Check for Producent en Product Nodes
            if node_label in product_component_types:
                product_node, row = get_product_node(row)
            else:
                product_node = ''   # Set product node to dummy
            # Remember uitdovend, uitgedoofd EOL dates
            uitdovend_datum = row['UITDOVEND_DATUM']
            uitgedoofd_datum = row['UITGEDOOFD_DATUM']
            del row['UITDOVEND_DATUM']
            del row['UITGEDOOFD_DATUM']
            valuedict = {}
            for attrib in row.keys():
                if row[attrib]:
                        valuedict[attrib.lower()] = str(row[attrib])
            component = ns.create_node(node_label, **valuedict)
            # Add link to location if location is known.
            if str(type(on_location)) == "<class 'py2neo.types.Node'>":
                ns.create_relation(component, 'Location', on_location)
            # Add link to product node if product is known
            if str(type(product_node)) == "<class 'py2neo.types.Node'>":
                ns.create_relation(component, 'Brand', product_node)
            link_eol(component, uitdovend_datum, uitgedoofd_datum)
        # Remember component for Relation in next step
        # noinspection PyUnresolvedReferences
        node_obj[row["CMDB_ID"]] = component
        node_info.info_loop()
    node_info.end_loop()

    # Handle relations
    rows = ds.get_relations()
    rels = {
            'heeft component': 'component',
            'is afhankelijk van': 'afhankelijk',
            'maakt gebruik van': 'gebruik'
    }
    rel_info = my_env.LoopInfo("Relations", 1000)
    for row in rows:
        ns.create_relation(node_obj[row["cmdb_id_source"]], rels[row['relation']], node_obj[row["cmdb_id_target"]])
        rel_info.info_loop()
    rel_info.end_loop()
    logging.info('End Application')
