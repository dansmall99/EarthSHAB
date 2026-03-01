from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
import glob
import os

#from datetime import MonkeyPatch
#MonkeyPatch.patch_fromisoformat()     # Hacky solution for Python 3.6 to use ISO format Strings

run_mane = 'SHAB9'
balloon_properties = dict(
    shape = 'sphere',
    d = 5.81,                          # (m) Diameter of Sphere Balloon
    mp = 1.9,                         # (kg) Mass of Payload
    areaDensityEnv = 939.*7.62E-6,    # (Kg/m^2) rhoEnv*envThickness
    mEnv = 2.0,                       # (kg) Mass of Envelope - SHAB6
    cp = 2000.,                       # (J/(kg K)) Specific heat of envelope material
    absEnv = .98,                     # Absorbiviy of envelope material
    emissEnv = .95,                   # Emisivity of enevelope material
    Upsilon = 4.5,                    # Ascent Resistance coefficient
)

#forecast_start_time = "2021-03-29 12:00:00" # Forecast start time, should match a downloaded forecast
#start_time = datetime.fromisoformat("2021-03-29 11:32:00") # Simulation start time. The end time needs to be within the downloaded forecast
#balloon_trajectory = None

#SHAB10
#forecast_start_time = "2022-04-09 12:00:00" # Forecast start time, should match a downloaded forecast
#start_time = datetime.fromisoformat("2022-04-09 18:14:00") # Simulation start time. The end time needs to be within the downloaded forecast
#balloon_trajectory = "balloon_data/SHAB10V-APRS.csv"  # Only Accepting Files in the Standard APRS.fi format for now

#SHAB3
#forecast_start_time = "2020-11-20 06:00:00" # Forecast start time, should match a downloaded forecast in the forecasts directory
#start_time = datetime.fromisoformat("2020-11-20 15:47:00") # Simulation start time. The end time needs to be within the downloaded forecast
#balloon_trajectory = "balloon_data/SHAB3V-APRS.csv"  # Only Accepting Files in the Standard APRS.fi format for now

#SHAB5
#forecast_start_time = "2021-05-12 12:00:00" # Forecast start time, should match a downloaded forecast in the forecasts directory
#start_time = datetime.fromisoformat("2021-05-12 14:01:00") # Simulation start time. The end time needs to be within the downloaded forecast
#balloon_trajectory = "balloon_data/SHAB5V_APRS_Processed.csv"  # Only Accepting Files in the Standard APRS.fi format for now

#SHAB14-V Example for EarthSHAB software
forecast_start_time =  "2024-09-27 12:00:00" # Forecast start time, should match a downloaded forecast in the forecasts directory
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

# Forecast start time, should match a downloaded forecast in the forecasts directory
forecast_start_time =  year + '-' + month + '-' + day + ' ' + hourstamp + ':00:00' #"2024-09-27 12:00:00" 
print('forecast_start_time = ' + forecast_start_time)

#forecast_start_time =  '2024-10-09 12:00:00' 

#launch start time UTC
start_time = datetime.fromisoformat("2025-10-10 15:30:00") # Simulation start time. The end time needs to be within the downloaded forecast
balloon_trajectory = None

#Hawaii
#forecast_start_time = "2023-04-18 00:00:00" # Forecast start time, should match a downloaded forecast
#start_time = datetime.fromisoformat("2023-04-18 18:00:00") # Simulation start time. The end time needs to be within the downloaded forecast
#balloon_trajectory = None  # Only Accepting Files in the Standard APRS.fi format for now


forecast = dict(
    forecast_type = "GFS",      # GFS or ERA5
    forecast_start_time = forecast_start_time, # Forecast start time, should match a downloaded forecast in the forecasts directory
    GFSrate = 60,               # (s) After how many iterated dt steps are new wind speeds are looked up
)

#These parameters are for both downloading new forecasts, and running simulations with downloaded forecasts.
netcdf_gfs = dict(
    #DO NOT CHANGE
    nc_file = ("forecasts/gfs_0p25_" + forecast['forecast_start_time'][0:4] + forecast['forecast_start_time'][5:7] + forecast['forecast_start_time'][8:10] + "_" + forecast['forecast_start_time'][11:13] + ".nc"),  # DO NOT CHANGE -  file structure for downloading .25 resolution NOAA forecast data.
    nc_start = datetime.fromisoformat(forecast['forecast_start_time']),    # DO NOT CHANGE - Start time of the downloaded netCDF file
    hourstamp = forecast['forecast_start_time'][11:13],  # parsed from gfs timestamp

    res = 0.25,        # (deg) DO NOT CHANGE

    #The following values are for savenetcdf.py for forecast downloading and saving
    lat_range = 30,    # (.25 deg)
    lon_range= 40,     # (.25 deg)
    download_days = 10, # (1-10) Number of days to download for forecast This value is only used in saveNETCDF.py
)

netcdf_era5 = dict(
    #filename = "SHAB3V_era_20201120_20201121.nc", #SHAB3
    #filename = "SHAB5V-ERA5_20210512_20210513.nc", #SHAB5V
    #filename = "shab10_era_2022-04-09to2022-04-10.nc", #SHAB10V
    filename = "SHAB14V_ERA5_20220822_20220823.nc", #SHAB12/13/14/15V
    #filename = "hawaii-ERA5-041823.nc",
    resolution_hr = 1
    )

simulation = dict(
    start_time = start_time,    # (UTC) Simulation Start Time, updated above
    sim_time = 18,              # (int) (hours) Number of hours to simulate

    vent = 0.0,                 # (kg/s) Vent Mass Flow Rate  (Do not have an accurate model of the vent yet, this is innacurate)
    alt_sp = 15000.0,           # (m) Altitude Setpoint
    v_sp = 0.,                  # (m/s) Altitude Setpoint, Not Implemented right now
    start_coord =	{ 
                      #"lat": 33.635050, #cms 35.1271, #33.66, #21.4, # 34.60,             # (deg) Latitude
                      #"lon": -103.972350, # cms-106.570633, #-114.22, #-158, #-106.80,           # (deg) Longitude
                      #"lat": 35.177864, #DRMS 33.66, #21.4, # 34.60,             # (deg) Latitude
                      #"lon": -106.547857, #DRMS -114.22, #-158, #-106.80,           # (deg) Longitude
                      "lat": 35.19605, #BFP 
                      "lon": -106.59733, #BFP
                      #"lat": 35.096440, #AHS
                      #"lon": -106.636764, #AHS
                      "alt": 1553.,             # (m) Elevation
                      "timestamp": start_time,  # current timestamp
                    },
    min_alt = 1646.,            # starting altitude. Generally the same as initial coordinate
    float = 25000,              # for simulating in trapezoid.py
    dt = 1.0,                   # (s) Integration timestep for simulation (If error's occur, use a lower step size)

    balloon_trajectory = balloon_trajectory # Default is None. Only accepting trajectories in aprs.fi csv format.
)

earth_properties = dict(
    Cp_air0 = 1003.8,           # (J/Kg*K)  Specifc Heat Capacity, Constant Pressure
    Cv_air0 = 716.,             # (J/Kg*K)  Specifc Heat Capacity, Constant Volume
    Rsp_air = 287.058,          # (J/Kg*K) Gas Constant
    P0 = 101325.0,              # (Pa) Pressure @ Surface Level
    emissGround = .95,          # assumption
    albedo = 0.17,              # assumption
)
