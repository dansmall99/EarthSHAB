for x in `seq 45`; do python3 autoNETCDF.py; python3 main.py;sleep 6h;rm forecasts/gfs*.nc; done
