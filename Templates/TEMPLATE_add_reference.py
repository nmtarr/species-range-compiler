"""
Adds a new reference to a GAP database.

Reference types:
A = journal article (primary literature)
B = book/monograph (primary literature)
D = database (such as Birds of the World)
N = non-refereed article (thesis, note)
O = official document
P = personal communication
R = report
U = unknown
W = Website
"""
import sys
sys.path.append("ENTER PATH HERE/gapproduction/")
from gapproduction import citations
# ------------------------------------------------------------------------------
# Define the reference textstring 
reference = "Burnham KP, Anderson DR (2029) Model THIS IS AN EXAMPLE approach. Springer-Verlag, Germany"
reference_type = 'B'
# ------------------------------------------------------------------------------
# Designate a dabase (2001 or 2016 GAP db) 
db = 'GapVert_48_2016_test'

# Check if reference already exists 
taken, matches = citations.CitationExists(reference, db)

# If you review the results (matches) and determine that it isn't actually 
# in the database yet, you can set taken to False here to add it.
taken = taken #False

# If it doesn't exist, add it 
if not taken:
    # Build reference code 
    reference_code = citations.BuildStrRefCode(reference, reference_type, db)

    # Add reference to database 
    citations.AddReference(reference, reference_code, db)

