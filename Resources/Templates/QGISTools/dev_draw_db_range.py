"""
N. Tarr 4/11/2023

For a given species, draws up entries in the GAP database.

You need to have a sqlite version of the hucs drawn up in QGIS.  NT has it 
at T:/Datasets/huc12rng_gap_polygon.sqlite with layername=huc12rng_gap_polygon
in QGIS.
"""
#-------------------------------------------------------------------------------
species_code = "mAMMAx"
group_name = "2016 Range - " + species_code
#-------------------------------------------------------------------------------
import pandas as pd
import processing
import sqlite3

# Import the gapproduction package to get the connection object
import sys
sys.path.append("REPLACETHIS/Code/GAPProduction")
from gapproduction import database, citations

# A dictionary of 2001 range codes
RangeCodesDict2001 = {"Presence": {1: "Known/extant",
                                   2: "Possibly present",
                                   3: "Potential for presence",
                                   4: "Extirpated/historical presence",
                                   5: "Extirpated purposely (applies to introduced species only)",
                                   6: "Occurs on indicated island chain",
                                   7: "Unknown"},
                "Origin": {1: "Native", 2: "Introduced", 3: "Either introduced or native",
                           4: "Reintroduced", 5: "Either introduced or reintroduced",
                           6: "Vagrant", 7: "Unknown"},
                "Reproduction": {1: "Breeding", 2: "Nonbreeding",
                                 3: "Both breeding and nonbreeding", 7: "Unknown"},
                 "Season": {1: "Year-round", 2: "Migratory", 3: "Winter", 4: "Summer",
                            5: "Passage migrant or wanderer", 6: "Seasonal permanence uncertain",
                            7: "Unknown", 8: "Vagrant"}}

# Connect to range db
cursor, connection = database.ConnectDB_pyodbc("GapVert_48_2016_test")

# ADD GROUPS TO TABLE OF CONTENTS ----------------------------------------------
root = QgsProject.instance().layerTreeRoot()

# Remove existing
for group in [child for child in root.children() if child.nodeType() == 0]:
    if group.name() == group_name:
        root.removeChildNode(group)

# Add new groups
node_group1 = root.addGroup(group_name)
node_subgroup1 = node_group1.addGroup("PRESENCE")
node_subgroup2 = node_group1.addGroup("SEASON")

# GET RANGE RECORDS ------------------------------------------------------------
# Query records and load as a layer
sql = f"""SELECT * FROM tblRanges WHERE strUC = '{species_code}';"""
df = pd.read_sql(sql, con=connection)
# You have to save the data frame to disk in order to make it a vector layer
df.to_csv("T:/temp/kjdfq.csv")
uri = "file:///T:/temp/kjdfq.csv?delimiter=,"
layer = QgsVectorLayer(uri, "2016", "delimitedtext")
QgsProject.instance().addMapLayer(layer)

# JOIN THE TABLE TO THE HUCS ---------------------------------------------------
# Join range records with huc polygons
params = {'INPUT': 'huc12rng_gap_polygon', 'FIELD': 'HUC12RNG',
            'INPUT_2': '2016', 'FIELD_2': 'strHUC12RNG',
            'OUTPUT': 'memory:'}
result = processing.run("native:joinattributestable", params)
layer2 = result['OUTPUT']

# ADD JOINED TABLE TO GROUPS ---------------------------------------------------
QgsProject.instance().addMapLayer(layer2, False)
layer2.setName("2016")
node_subgroup1.addLayer(layer2)
node_subgroup2.addLayer(layer2)

# Remove the table
node_subgroup1.removeLayer(layer)
node_subgroup2.removeLayer(layer)
QgsProject.instance().removeMapLayer(layer)

# SYMBOLOGY --------------------------------------------------------------------
symbol_props = {'border_width_map_unit_scale': '3x:0,0,0,0,0,0', 
                'color': '0,0,0,255', 'joinstyle': 'bevel', 'offset': '0,0', 
                'offset_map_unit_scale': '3x:0,0,0,0,0,0', 'offset_unit': 'MM', 
                'outline_color': '35,35,35,255', 'outline_style': 'solid', 
                'outline_width': '0.0', 'outline_width_unit': 'MM', 
                'style': 'solid'}
                
# Presence -----
renderer = QgsCategorizedSymbolRenderer()

# Add categories
cat_colors = {1: '14,100,40,255',2: '158,74,57,255',
              3: '16,166,40,255',4: '175,75,57,255',
              5: '11,166,40,255',6: '179,79,57,255',
              7: '19,166,40,255'}
                
cat_name = RangeCodesDict2001["Presence"]

# Edit the renderer
for cat in cat_name:
        name = cat_name[cat]
        color = cat_colors[cat]
        # Customize symbol
        props = symbol_props.copy()
        props['color'] = cat_colors[cat]
        props['outline_color'] = cat_colors[cat]
        symbol = QgsFillSymbol().createSimple(props)
        renderer.addCategory(QgsRendererCategory(cat, symbol, name))
                
if renderer is not None:
    renderer.setClassAttribute("intGapPres")
    
# repaint the layer
layer2.setRenderer(renderer)
layer2.triggerRepaint()

#QgsProject.instance().layerTreeRoot().findLayer(layer2).setItemVisibilityChecked(False)
#QgsProject.instance().layerTreeRoot().findLayer(layer2).setExpanded(False)

# Season -----
# Symbology
renderer = QgsCategorizedSymbolRenderer()

# Add categories
cat_colors = {1: '14,166,40,200',2: '158,74,57,255',
              3: '16,166,40,255',4: '175,75,57,255',
              5: '11,166,40,255',6: '179,79,57,255',
              7: '19,166,40,255', 8: '180, 80, 60, 200'}
                
cat_name = RangeCodesDict2001["Season"]

# Edit the renderer
for cat in cat_name:
        name = cat_name[cat]
        color = cat_colors[cat]
        # Customize symbol
        props = symbol_props.copy()
        props['color'] = cat_colors[cat]
        props['outline_color'] = cat_colors[cat]
        symbol = QgsFillSymbol().createSimple(props)
        renderer.addCategory(QgsRendererCategory(cat, symbol, name))
                
if renderer is not None:
    renderer.setClassAttribute("intGapSeas")
    
# repaint the layer
layer2.setRenderer(renderer)
layer2.triggerRepaint()

QgsProject.instance().layerTreeRoot().findLayer(layer2).setItemVisibilityChecked(False)
QgsProject.instance().layerTreeRoot().findLayer(layer2).setExpanded(False)

print("COMPLETE")