"""
N. Tarr 4/11/2023

For a given species, draws up entries in the range_opinions database.
"""
#-------------------------------------------------------------------------------
species_code = "mAMMAx"
years = ["2006"]
group_name = "Opinions"
#-------------------------------------------------------------------------------
import pandas as pd
import processing
import sqlite3

# Connect to range db
connection = sqlite3.connect("REPLACETHIS/Vert/DBase/range_opinions.sqlite")

# ADD GROUPS -------------------------------------------------------------------
root = QgsProject.instance().layerTreeRoot()

# Remove existing
for group in [child for child in root.children() if child.nodeType() == 0]:
    if group.name() == group_name:
        root.removeChildNode(group)

# Add new groups
node_group1 = root.addGroup(group_name)
node_subgroup1 = node_group1.addGroup("PRESENCE")
node_subgroup2 = node_group1.addGroup("WINTER")
node_subgroup3 = node_group1.addGroup("SUMMER")
node_subgroup4 = node_group1.addGroup("YEAR_ROUND")

# DEFINE FUNCTION TO DRAW ------------------------------------------------------
def draw(table, years):
    for year in years:
        print(table, "--", year)
        # Query records and get loaded as a layer
        sql = """SELECT * FROM {2} 
                 WHERE species_code = '{0}' 
                 AND year == {1};""".format(species_code, year, table)
        df = pd.read_sql(sql, con=connection)
        df.to_csv("REPLACETHIS/temp/temp_draw.csv")
        uri = "file:///REPLACETHIS/temp/temp_draw.csv?delimiter=,"
        layer = QgsVectorLayer(uri, year, "delimitedtext")
        QgsProject.instance().addMapLayer(layer)
        if table == 'presence':
            node_subgroup1.addLayer(layer)
        if table == 'winter':
            node_subgroup2.addLayer(layer)
        if table == 'summer':
            node_subgroup3.addLayer(layer)
        if table == 'year_round':
            node_subgroup4.addLayer(layer)
        
        # Join opinion records with huc polygons
        params = {'INPUT': 'huc12rng_gap_polygon', 'FIELD': 'HUC12RNG',
                  'INPUT_2': year, 'FIELD_2': 'strHUC12RNG',
                  'OUTPUT': 'memory:'}
        result = processing.run("native:joinattributestable", params)
        layer2 = result['OUTPUT']
        QgsProject.instance().addMapLayer(layer2, False)
        if table == 'presence':
            node_subgroup1.addLayer(layer2)
        if table == 'winter':
            node_subgroup2.addLayer(layer2)
        if table == 'summer':
            node_subgroup3.addLayer(layer2)
        if table == 'year_round':
            node_subgroup4.addLayer(layer2)
        layer2.setName(year)
        
        # Remove the table
        if table == 'presence':
            node_subgroup1.removeLayer(layer)
        if table == 'winter':
            node_subgroup2.removeLayer(layer)
        if table == 'summer':
            node_subgroup3.removeLayer(layer)
        if table == 'year_round':
            node_subgroup4.removeLayer(layer)
        QgsProject.instance().removeMapLayer(layer)
        
        # Symbology
        renderer = QgsCategorizedSymbolRenderer()

        # Add categories
        cat_colors = {'present': '14,166,40,255','absent': '158,74,57,255'}
                      
        cat_name = {'present': 'present','absent': 'absent'}

        symbol_props = {'border_width_map_unit_scale': '3x:0,0,0,0,0,0', 
                        'color': '0,0,0,255', 'joinstyle': 'bevel', 'offset': '0,0', 
                        'offset_map_unit_scale': '3x:0,0,0,0,0,0', 'offset_unit': 'MM', 
                        'outline_color': '35,35,35,255', 'outline_style': 'solid', 
                        'outline_width': '0.0', 'outline_width_unit': 'MM', 
                        'style': 'solid'}

        # Edit the renderer
        for cat in ['present', 'absent']:
                name = cat_name[cat]
                color = cat_colors[cat]
                # Customize symbol
                props = symbol_props.copy()
                props['color'] = cat_colors[cat]
                props['outline_color'] = cat_colors[cat]
                symbol = QgsFillSymbol().createSimple(props)
                renderer.addCategory(QgsRendererCategory(cat, symbol, name))
                        
        if renderer is not None:
            renderer.setClassAttribute("status")
            
        # repaint the layer
        layer2.setRenderer(renderer)
        layer2.triggerRepaint()
        QgsProject.instance().layerTreeRoot().findLayer(layer2).setItemVisibilityChecked(False)
        QgsProject.instance().layerTreeRoot().findLayer(layer2).setExpanded(False)

# PRESENCE ---------------------------------------------------------------------
draw('presence', years)

# WINTER -----------------------------------------------------------------------
draw('winter', years)

# SUMMER -----------------------------------------------------------------------
draw('summer', years)

# YEAR_ROUND -------------------------------------------------------------------
draw('year_round', years)
print("COMPLETE")