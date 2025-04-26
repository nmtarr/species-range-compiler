"""
N. Tarr May 14, 2023

Adds new group for binary output from the simple_results table in the range
output database.  Layers are subgrouped by year.
"""

group_name = "Pacific Marten Binary Output"
sp_code = "mPAMAx"
range_db = "REPLACETHIS/Workspaces/RangeMaps/marten/mAMMAx2016.sqlite"
periods = [2025, 2020, 2015, 2010, 2005]
seasons = ["summer", "winter", "year_round"].append("presence")

"""**************************************************************************"""
import sqlite3
from PyQt5.QtGui import *
from random import randrange
import numpy as np
import pandas as pd

"""# ADD A GROUP ------------------------------------------------------------------
root = QgsProject.instance().layerTreeRoot()

# Remove existing
for group in [child for child in root.children() if child.nodeType() == 0]:
    if group.name() == group_name:
        root.removeChildNode(group)"""

# Add new groups for each of the columns types in simple_results
# Find the presence and season that are represented in the table
connection = sqlite3.connect(range_db)
sql = """ SELECT * FROM simple_results; """
df = pd.read_sql(sql, connection)
columns = df.columns[1:].copy()

# Which season names can be found in the column names?
seasons = set([x.split("_20")[0] for x in columns])

print(seasons)

subgroup_dict = {"Presence": "node_subgroup1",
                 "Summer": "node_subgroup2",
                 "Winter": "node_subgroup3",    
                 "Year_Round": "node_subgroup4"}


"""
node_group1 = root.addGroup(group_name)
node_subgroup1 = node_group1.addGroup("PRESENCE")


# Connect to range db
uri = QgsDataSourceUri()
uri.setDatabase(range_db)
schema = ''


# LOAD PRESENCE ----------------------------------------------------------------
table = 'presence'
geom_column = 'geom_5070'
uri.setDataSource(schema, table, geom_column)

# Load presence layers
for period in [2025, 2020, 2015, 2010, 2005]:
    layer_name = sp_code + " " + str(period)
    layer = QgsVectorLayer(uri.uri(), layer_name, 'spatialite')
    
    if not layer.isValid():
        print("Layer failed to load!")
    else:
        QgsProject.instance().addMapLayer(layer, False)
        node_subgroup1.addLayer(layer)
    
    field_nameP = "presence_" + str(period)"
    renderer = QgsCategorizedSymbolRenderer()

    # Add categories
    cat_colors = {'1': '99,75,103,255','2'}
                  
    cat_name = {'1': 'Included'}

    symbol_props = {'border_width_map_unit_scale': '3x:0,0,0,0,0,0', 
                    'color': '0,0,0,255', 'joinstyle': 'bevel', 'offset': '0,0', 
                    'offset_map_unit_scale': '3x:0,0,0,0,0,0', 'offset_unit': 'MM', 
                    'outline_color': '35,35,35,255', 'outline_style': 'solid', 
                    'outline_width': '0.0', 'outline_width_unit': 'MM', 
                    'style': 'solid'}

      
    # Edit the renderer
    for cat in ['1']:
            name = cat_name[cat]
            color = cat_colors[cat]
            # Customize symbol
            props = symbol_props.copy()
            props['color'] = cat_colors[cat]
            props['outline_color'] = cat_colors[cat]
            symbol = QgsFillSymbol().createSimple(props)
            renderer.addCategory(QgsRendererCategory(cat, symbol, name))
                    
    if renderer is not None:
        renderer.setClassAttribute(field_nameP)
        
    # repaint the layer
    layer.setRenderer(renderer)
    layer.triggerRepaint()
    QgsProject.instance().layerTreeRoot().findLayer(layer).setItemVisibilityChecked(False)
    QgsProject.instance().layerTreeRoot().findLayer(layer).setExpanded(False)

# MOVE UP IN TOC --------------------------------------------------------------
cloned_group1 = node_group1.clone()
root.insertChildNode(1, cloned_group1)
root.removeChildNode(node_group1)
"""