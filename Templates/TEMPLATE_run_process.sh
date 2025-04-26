# This bash script runs a sequence of python scripts for the compiling and 
# testing of GAP range data. 
# Fill out accordingly and add or remove lines to run weighting scripts as
# needed.
# For ww_outputs and seasons, if more than one is needed, separate with a comma
# within the quotes (e.g. "S,W").
# If you are not using occurrence data, you can set ww_outputs to
# $scriptdir/NORECORDS.sqlite to avoid some error messages.
# N. Tarr, January 29, 2023.

species="mFISHx"
task_name="Gen2"
shorthand="Fisher"
seasons="Y"
change_csv=FALSE
initials="NMT"

workdir="C:/orkpes/RangeMaps/"
scriptdir="T:/Code/spp-ranges-nc/"
wranglerdir="T:/Code/wildlife-wrangler/"

ww_outputs=$scriptdir/NORECORDS.sqlite
grid="C:/Datasets/huc12rng_gap_polygon.sqlite"

python $workdir/mFISHx0_weighting.py $ww_outputs

python $scriptdir/GAP-range-compiler.py $shorthand $species $task_name $seasons $initials $workdir $ww_outputs $scriptdir $gapproddir $wranglerdir $hucs
python $scriptdir/Tests/output-tests.py $species $task_name $workdir
python $scriptdir/range-change-summary.py $species $task_name $workdir $change_csv