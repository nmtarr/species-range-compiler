"""
Loads GAP presence data for a species.  Creates a new group with occurrence 
records, GAP presence for each time period, and weight of evidence for each
time period.

N. Tarr, 4/9/2022

ISSUES
------
Also, doesn't work when I try to keep renderer outside of the loop.
"""
group_name = "Fisher Presence"
sp_code = "mFISHx"
range_db = "REPLACETHIS/Workspaces/RangeMaps/fisher/mFISHxGen2.sqlite"
periods = [2025, 2020, 2015, 2010, 2005]

"""**************************************************************************"""
import sqlite3
from PyQt5.QtGui import *
from random import randrange
import numpy as np

# ADD A GROUP ------------------------------------------------------------------
root = QgsProject.instance().layerTreeRoot()

# Remove existing
for group in [child for child in root.children() if child.nodeType() == 0]:
    if group.name() == group_name:
        root.removeChildNode(group)

# Add new groups
node_group1 = root.addGroup(group_name)
node_subgroup1 = node_group1.addGroup("PRESENCE CODES")
node_subgroup2 = node_group1.addGroup("OCCURRENCE RECORDS")
node_subgroup3 = node_group1.addGroup("SUMMED RECORD WEIGHTS")
node_subgroup4 = node_group1.addGroup("EXPERT OPINIONS")
node_subgroup5 = node_group1.addGroup("EXPERT OPINION WEIGHT")
node_subgroup6 = node_group1.addGroup("AGE OF LAST RECORD")
node_subgroup7 = node_group1.addGroup("GAP Version 1 (2001)")

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
    
    field_nameP = "presence_" + str(period)
    renderer = QgsCategorizedSymbolRenderer()

    # Add categories
    cat_colors = {'1': '99,75,103,255','2': '185,160,196,255',
                  '3': '197,187,201,255', '4': '228,184,52,255',
                  '5': '189,135,34,255'}
    cat_colors = {'1': '44,162,95,255','2': '153,216,201,255',
                  '3': '229,245,249,255', '4': '150,150,150,255',
                  '5': '99,99,99,255'}
                  
    cat_name = {'1': 'Confirmed present','2': 'Likely present',
                '3': 'Suspected present','4': 'Suspected absent',
                '5': 'Likely absent'}

    symbol_props = {'border_width_map_unit_scale': '3x:0,0,0,0,0,0', 
                    'color': '0,0,0,255', 'joinstyle': 'bevel', 'offset': '0,0', 
                    'offset_map_unit_scale': '3x:0,0,0,0,0,0', 'offset_unit': 'MM', 
                    'outline_color': '35,35,35,255', 'outline_style': 'solid', 
                    'outline_width': '0.0', 'outline_width_unit': 'MM', 
                    'style': 'solid'}

      
    # Edit the renderer
    for cat in ['1', '2', '3', '4', '5']:
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
    

# LOAD OCCURRENCE RECORDS ------------------------------------------------------
table = 'occurrence_records'
geom_column = 'geometry'
try:
    uri.setDataSource(schema, table, geom_column)
    layer_name = "Records"
    layer = QgsVectorLayer(uri.uri(), layer_name, 'spatialite')

    if not layer.isValid():
        print("Layer failed to load!")
    else:
        QgsProject.instance().addMapLayer(layer, False)
        node_subgroup2.addLayer(layer)
     
    mySymbol1=QgsFillSymbol.createSimple({'color':'#289e26',
                                          'color_border':'#289e26',
                                          'width_border':'.5',
                                          'style':'no'})
    myRenderer = layer.renderer()
    myRenderer.setSymbol(mySymbol1)
    layer.triggerRepaint()
    QgsProject.instance().layerTreeRoot().findLayer(layer).setItemVisibilityChecked(False)
    QgsProject.instance().layerTreeRoot().findLayer(layer).setExpanded(False)
except:
    print("No Occurrence Records")

# LOAD SUMMED RECORD WEIGHT ----------------------------------------------------
table = 'presence'
geom_column = 'geom_5070'
uri.setDataSource(schema, table, geom_column)

for period in [2025, 2020, 2015, 2010, 2005]:
    layer_name = sp_code + " " + str(period)
    layer = QgsVectorLayer(uri.uri(), layer_name, 'spatialite')
    
    if not layer.isValid():
        print("Layer failed to load!")
    else:
        QgsProject.instance().addMapLayer(layer, False)
        node_subgroup3.addLayer(layer)
     
    field_nameW = "recent_weight_" + str(period)

    # Build range categories
    range_list= []

    ranges = {"0-1": (0.0, 1.0), "1-2": (1.1, 2.0), "2-3": (2.1, 3.0),
                 "3-4": (3.1, 4.0), "4-5": (4.1, 5.0), "5-6": (5.1, 6.0),
                 "6-7": (6.1, 7.0), "7-8": (7.1, 8.0), "8-9": (8.1, 9.0),
                 "9-10": (9.1, 10.0), "10-13": (10.1, 13.0), "13-20": (13.1, 20.0),
                 "20-40": (20.1, 40.0), "40-60": (40.1, 60.0), "60-100": (60.1, 100),
                 "10+": (100.1, 10000.0)}

    for grp in ranges.keys():
        symbol = QgsFillSymbol()
        symbol.symbolLayer(0).setStrokeStyle(Qt.PenStyle(Qt.NoPen))
        range = QgsRendererRange(ranges[grp][0], ranges[grp][1], symbol, grp)
        range_list.append(range)

    # Build renderer with range categories
    renderer = QgsGraduatedSymbolRenderer('', range_list)
    classification_method= QgsApplication.classificationMethodRegistry().method("EqualInterval")
    renderer.setClassificationMethod(classification_method)
    renderer.setClassAttribute(field_nameW)

    # Apply color ramp
    default_style = QgsStyle().defaultStyle()
    color_ramp = default_style.colorRamp("Spectral")
    color_ramp.invert()
    renderer.updateColorRamp(color_ramp)

    # Repaint the layer
    layer.setRenderer(renderer)
    layer.triggerRepaint()
    QgsProject.instance().layerTreeRoot().findLayer(layer).setItemVisibilityChecked(False)
    QgsProject.instance().layerTreeRoot().findLayer(layer).setExpanded(False)
    

# LOAD OPINION WEIGHT ----------------------------------------------------------
table = 'presence'
geom_column = 'geom_5070'
uri.setDataSource(schema, table, geom_column)
for period in periods:
    layer_name = sp_code + " " + str(period)
    layer = QgsVectorLayer(uri.uri(), layer_name, 'spatialite')
    
    if not layer.isValid():
        print("Layer failed to load!")
    else:
        QgsProject.instance().addMapLayer(layer, False)
        node_subgroup5.addLayer(layer)
     
    field_nameW = "opinion_" + str(period) + "_weight"

    # Build range categories
    range_list= []

    ranges = {"1-2": (1.0, 1.999), "2-3": (2.0, 2.999),
                 "3-4": (3.0, 3.999), "4-5": (4.0, 4.999), "5-6": (5.0, 5.999),
                 "6-7": (6.0, 6.999), "7-8": (7.0, 7.999), "8-9": (8.0, 8.999),
                 "9-10": (9.0, 10.0)}

    for grp in ranges.keys():
        symbol = QgsFillSymbol()
        symbol.symbolLayer(0).setStrokeStyle(Qt.PenStyle(Qt.NoPen))
        range = QgsRendererRange(ranges[grp][0], ranges[grp][1], symbol, grp)
        range_list.append(range)

    # Build renderer with range categories
    renderer = QgsGraduatedSymbolRenderer('', range_list)
    classification_method= QgsApplication.classificationMethodRegistry().method("EqualInterval")
    renderer.setClassificationMethod(classification_method)
    renderer.setClassAttribute(field_nameW)

    # Apply color ramp
    default_style = QgsStyle().defaultStyle()
    color_ramp = default_style.colorRamp("Greys")
    renderer.updateColorRamp(color_ramp)

    # Repaint the layer
    layer.setRenderer(renderer)
    layer.triggerRepaint()
    QgsProject.instance().layerTreeRoot().findLayer(layer).setItemVisibilityChecked(False)
    QgsProject.instance().layerTreeRoot().findLayer(layer).setExpanded(False)
    

# LOAD OPINIONS ----------------------------------------------------------------
table = 'presence'
geom_column = 'geom_5070'
uri.setDataSource(schema, table, geom_column)

for period in [2025, 2020, 2015, 2010, 2005]:
    layer_name = sp_code + " " + str(period)
    layer = QgsVectorLayer(uri.uri(), layer_name, 'spatialite')
    
    if not layer.isValid():
        print("Layer failed to load!")
    else:
        QgsProject.instance().addMapLayer(layer, False)
        node_subgroup4.addLayer(layer)
     
    field_nameOP = "opinion_" + str(period)
    
    categorized_renderer = QgsCategorizedSymbolRenderer()
    # Add a few categories
    cat1_symbol = QgsFillSymbol.createSimple({'color':'#FFC000',
                                                'color_border':'#FFC000',
                                                'width_border':'1',
                                                'style':'no'})
    cat2_symbol = QgsFillSymbol.createSimple({'color':'#0D0D0D',
                                                'color_border':'#0D0D0D',
                                                'width_border':'1',
                                                'style':'no'})
    cat1 = QgsRendererCategory('1', cat1_symbol, 'present')
    cat2 = QgsRendererCategory('0', cat2_symbol, 'absent')
    categorized_renderer.addCategory(cat1)
    categorized_renderer.addCategory(cat2)

    if categorized_renderer is not None:
        categorized_renderer.setClassAttribute(field_nameOP)
        
    # repaint the layer
    layer.setRenderer(categorized_renderer)
    layer.triggerRepaint()
    QgsProject.instance().layerTreeRoot().findLayer(layer).setItemVisibilityChecked(False)
    QgsProject.instance().layerTreeRoot().findLayer(layer).setExpanded(False)
    

# LOAD AGE IN WEEKS ------------------------------------------------------------
table = 'last_record'
geom_column = 'geom_5070'
uri.setDataSource(schema, table, geom_column)

layer_name = "Weeks"
layer = QgsVectorLayer(uri.uri(), layer_name, 'spatialite')

if not layer.isValid():
    print("Layer failed to load!")
    print("No age of last record available")
else:
    QgsProject.instance().addMapLayer(layer, False)
    node_subgroup6.addLayer(layer)
 
    field_nameL = "age_in_weeks"

    graduated_renderer = QgsGraduatedSymbolRenderer()

    # Add categories
    bins = list(np.arange(0,1300,50))
    for i in bins[1:]:
        start = bins[bins.index(i)-1]
        end = i
        graduated_renderer.addClassRange(QgsRendererRange(QgsClassificationRange('{0}-{1}'.format(start, end), 
                                                                                  start, end), 
                                                                                  QgsFillSymbol()))

    if graduated_renderer is not None:
        graduated_renderer.setClassAttribute(field_nameL)
        
    color_ramp = default_style.colorRamp("Viridis")
    color_ramp.invert()
    graduated_renderer.updateColorRamp(color_ramp)
        
    # repaint the layer
    layer.setRenderer(graduated_renderer)
    layer.triggerRepaint()
    QgsProject.instance().layerTreeRoot().findLayer(layer).setItemVisibilityChecked(False)
    QgsProject.instance().layerTreeRoot().findLayer(layer).setExpanded(False)


# LOAD GAP VERSION 1 -----------------------------------------------------------
table = 'presence'
geom_column = 'geom_5070'
uri.setDataSource(schema, table, geom_column)
layer_name = 'GAP Version 1 2001'
layer = QgsVectorLayer(uri.uri(), layer_name, 'spatialite')

if not layer.isValid():
    print("Layer failed to load!")
else:
    QgsProject.instance().addMapLayer(layer, False)
    node_subgroup7.addLayer(layer)

field_nameV1 = "presence_2001v1"
renderer = QgsCategorizedSymbolRenderer()

## Add categories
#cat_colors = {'1': '99,75,103,255', '2': '185,160,196,255',
#              '3': '197,187,201,255', '4': '228,184,52,255',
#              '5': '189,135,34,255', '6': '185,160,196,160', 
#              '7': '185,160,196,160'}
              
cat_name = {'1': "Known/extant",
            '2': "Possibly present",
            '3': "Potential for presence",
            '4': "Extirpated/historical presence",
            '5': "Extirpated purposely (applies to introduced species only)",
            '6': "Occurs on indicated island chain",
            '7': "Unknown"}

symbol_props = {'border_width_map_unit_scale': '3x:0,0,0,0,0,0', 
                'color': '0,0,0,255', 'joinstyle': 'bevel', 'offset': '0,0', 
                'offset_map_unit_scale': '3x:0,0,0,0,0,0', 'offset_unit': 'MM', 
                'outline_color': '35,35,35,255', 'outline_style': 'no', 
                'outline_width': '0.0', 'outline_width_unit': 'MM', 
                'style': 'solid'}

  
# Edit the renderer
for cat in ['1', '2', '3', '4', '5', '6', '7']:
        name = cat_name[cat]
#        color = cat_colors[cat]
        # Customize symbol
        props = symbol_props.copy()
#        props['color'] = cat_colors[cat]
#        props['outline_color'] = cat_colors[cat]
        symbol = QgsFillSymbol().createSimple(props)
        renderer.addCategory(QgsRendererCategory(cat, symbol, name))
                
if renderer is not None:
    renderer.setClassAttribute(field_nameV1)

# Apply color ramp
default_style = QgsStyle().defaultStyle()
color_ramp = default_style.colorRamp("Spectral")
color_ramp.invert()
renderer.updateColorRamp(color_ramp)

# repaint the layer
layer.setRenderer(renderer)
layer.triggerRepaint()
QgsProject.instance().layerTreeRoot().findLayer(layer).setItemVisibilityChecked(False)
QgsProject.instance().layerTreeRoot().findLayer(layer).setExpanded(False)

# MOVE UP IN TOC --------------------------------------------------------------
cloned_group1 = node_group1.clone()
root.insertChildNode(1, cloned_group1)
root.removeChildNode(node_group1)