# autoNETCDF.py

def generate_gfs_netcdf():
    import netCDF4 as nc4
    import config_earth
    from termcolor import colored
    import numpy as np
    import sys
    import requests
    from bs4 import BeautifulSoup
    import re
    import os

    coord = config_earth.simulation['start_coord']
    lat_range = config_earth.netcdf_gfs['lat_range']
    lon_range = config_earth.netcdf_gfs['lon_range']
    download_days = config_earth.netcdf_gfs['download_days']
    hourstamp = config_earth.netcdf_gfs['hourstamp']
    res = config_earth.netcdf_gfs['res']
    nc_start = config_earth.netcdf_gfs['nc_start']

    if not os.path.exists('forecasts'):
        os.makedirs('forecasts')

    def closest(arr, k):
        """ Given an ordered array and a value, determines the index of the closest item
        contained in the array.
        """
        return min(range(len(arr)), key = lambda i: abs(arr[i]-k))
    
    def getNearestLat(lat,min,max):
        """ Determines the nearest lattitude (to .25 degrees)
        """
        arr = np.arange(start=min, stop=max, step=res)
        i = closest(arr, lat)
        return i
    
    def getNearestLon(lon,min,max):
        """ Determines the nearest longitude (to .25 degrees)
        """
        lon = lon % 360 #convert from -180-180 to 0-360
        arr = np.arange(start=min, stop=max, step=res)
    
        i = closest(arr, lon)
        return i
    
    lat_i = getNearestLat(coord["lat"],-90,90.01)
    lon_i = getNearestLon(coord["lon"],0,360)
    
    coords = ['time', 'lat', 'lon', 'lev']
    vars_out = ['ugrdprs', 'vgrdprs', 'hgtprs', 'tmpprs']
    
    # parse GFS forecast start time from config file
    year = str(nc_start.year)
    month = str(nc_start.month).zfill(2)
    day = str(nc_start.day).zfill(2)
    
    
    # URL of the webpage to parse
    url = "https://nomads.ncep.noaa.gov/dods/gfs_0p25/"
    
    # Make a request to fetch the webpage content
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all 'dir' links in the webpage
    dir_links = [link.get('href') for link in soup.find_all('a') if 'dir' in link.get_text()]
    print(dir_links)
    
    # Get the last 'dir' link name
    last_dir = dir_links[-2] if dir_links else None
    print(last_dir)
    
    if last_dir:
        # Create the URL for the last 'dir' link
        dir_url = last_dir
    
        # Parse the content of the directory link
        dir_response = requests.get(dir_url)
        dir_soup = BeautifulSoup(dir_response.text, 'html.parser')
        #print(dir_soup)
        matches = dir_soup.find_all(string=re.compile(r'\b[1-8] entries'))
        if matches:
            print(matches[-1])
            s= str(matches[-1])
            i = s.index('entries')
            n = matches[-1][i-2]
            print('number = ' + n)
        if n == '1':
            last_dir = dir_links[-3]
            dir_url = last_dir
            dir_response = requests.get(dir_url)
            dir_soup = BeautifulSoup(dir_response.text, 'html.parser')
            #print(dir_soup)
            matches = dir_soup.find_all(string=re.compile(r'\b[1-8] entries'))
            if matches:
                print(matches[-1])
                s= str(matches[-1])
                i = s.index('entries')
                n = matches[-1][i-2]
                print('number = ' + n)
    
        gfsIndex = dir_url.rindex('gfs')
        print(str(gfsIndex))
        year = dir_url[gfsIndex+3:gfsIndex+7]
        month = dir_url[gfsIndex+7:gfsIndex+9]
        day = dir_url[gfsIndex+9:gfsIndex+11]
    
        if (n == '2' or n == '3'):
           t = '00'
        elif (n == '4' or n == '5'):
           t = '06'
        elif (n == '6' or n == '7'):
           t = '12'
        elif (n == '8' or n == '9'):
           t = '18'
        hourstamp = t
    
    print("Downloading data from")
    url = "https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs" + year + month + day + "/gfs_0p25_" + str(hourstamp) + "z"
    print(colored(url,"cyan"))
    
    # Open input file in read (r), and output file in write (w) mode:
    try:
        nc_in = nc4.Dataset(url)
    except:
        print(colored("NOAA DODS Server error with timestamp " + str(nc_start) + ". Data not downloaded.", "red"))
        sys.exit()
    
    nc_out = nc4.Dataset(config_earth.netcdf_gfs['nc_file'], 'w')
    
    for name, dimension in nc_in.dimensions.items():
        nc_out.createDimension(name, len(dimension) if not dimension.isunlimited() else None)
    
    for name, variable in nc_in.variables.items():
        if name in coords:
            x = nc_out.createVariable(name, variable.datatype, variable.dimensions)
            v = nc_in.variables[name][:]
            nc_out.variables[name][:] = v
            print ("Downloaded " + name)
    
        if name in vars_out:
            print ("Downloading " + name)
            x = nc_out.createVariable(name, variable.datatype, variable.dimensions, zlib=True) #Without zlib the file will be MASSIVE
    
            #Download only a chunk of the data
            for i in range(0,download_days*8+1):  #In intervals of 3 hours. hour_index of 8 is 8*3=24 hours. Add one more index to get full day range
                #print(lat_i,lon_i)
                data = nc_in.variables[name][i,0:34,lat_i-lat_range:lat_i+lat_range,lon_i-lon_range:lon_i+lon_range] #This array can only have a maximum of  536,870,912 elements, Need to dynamically add.
                nc_out.variables[name][i,0:34,lat_i-lat_range:lat_i+lat_range,lon_i-lon_range:lon_i+lon_range] = data
                print("Downloaded and added to output file ", name, ' hour index - ', i, ' time - ', i*3)
    
if __name__ == "__main__":
    # Optional: still allow CLI usage
    print("Generating GFS NetCDF via autoNETCDF.py...")
    fn = generate_gfs_netcdf()
    print("Wrote:", fn)
