""" This file shows an example of how to predict solar balloon trajectories and produces several plots
as well as an html trajectory map that uses Google maps and can be opened in an internet browser.

run saveNETCDF.py before running this file to download a forecast from NOAA.

Maybe convert to this new library later https://unidata.github.io/python-training/workshop/Bonus/downloading-gfs-with-siphon/

"""

import math
import solve_states
import GFS
from GFS import OutOfBoundsError
import ERA5
from termcolor import colored
import matplotlib.pyplot as plt
import fluids
import gmplot
import time as tm
import pandas as pd
import os
import numpy as np
import re
import copy
import simplekml

import seaborn as sns
import xarray as xr
from netCDF4 import Dataset
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

import radiation
import config_earth
import secret_keys
import windmap
import zipfile
import yagmail

from datetime import datetime



def elevation_to_color(elevation,maxAlt):
    """
    Converts an elevation value (0 to 30,000 meters) to a KML color string (aabbggrr).
    The color mapping is as follows:
    - 0 meters   -> Blue (ff0000ff)
    - 15,000 meters -> Green (ff00ff00)
    - 30,000 meters -> Red (ffff0000)

    Args:
        elevation (int): Elevation in meters (between 0 and 30,000).

    Returns:
        str: Color string in aabbggrr format for simplekml.
    """
    # Clamp the elevation to the range 0 to 30,000 meters
    elevation = max(0, min(elevation, maxAlt))

    # Determine the ratio in the spectrum
    ratio = elevation / float(maxAlt)  # Normalize to a value between 0 and 1

    if ratio <= 0.5:
        # Interpolate between Blue (0, 0, 255) to Green (0, 255, 0)
        blue = int(255 * (1 - 2 * ratio))
        green = int(255 * (2 * ratio))
        red = 0
    else:
        # Interpolate between Green (0, 255, 0) to Red (255, 0, 0)
        blue = 0
        green = int(255 * (2 - 2 * ratio))
        red = int(255 * (2 * ratio - 1))

    # Convert RGB values to hex and create KML color string in aabbggrr format
    return f'ff{blue:02x}{green:02x}{red:02x}'  # Always use full opacity 'ff'



def run_simulation(progress_callback=None, stop_check_callback=None, telemetry_callback=None):

   curDate = datetime.today().strftime('%Y-%m-%d_%H:%M:%S')
   if not os.path.exists('trajectories'):
       os.makedirs('trajectories')

   scriptstartTime = tm.time()

   GMT = 7 #0 # UTC MST
   dt = config_earth.simulation['dt']
   coord = config_earth.simulation['start_coord']
   t = config_earth.simulation['start_time']
   start = t
   nc_start = config_earth.netcdf_gfs["nc_start"]
   min_alt = config_earth.simulation['min_alt']
   alt_sp = config_earth.simulation['alt_sp']
   v_sp = config_earth.simulation['v_sp']
   sim_time = config_earth.simulation['sim_time'] * int(3600*(1/dt))
   lat = [coord["lat"]]
   lon = [coord["lon"]]
   GFSrate = config_earth.forecast['GFSrate']
   hourstamp = config_earth.netcdf_gfs['hourstamp']
   balloon_trajectory = config_earth.simulation['balloon_trajectory']
   forecast_type = config_earth.forecast['forecast_type']
   atm = fluids.atmosphere.ATMOSPHERE_1976(min_alt)

   forecast_file_name=config_earth.netcdf_gfs['nc_file'][10:-3]

   #Get trajectory name from config file for Google Maps:
   if balloon_trajectory != None:
       trajectory_name = copy.copy(balloon_trajectory)
       replacements=[("balloon_data/", ""), (".csv", "")]
       for pat,repl in replacements:
           trajectory_name = re.sub(pat, repl, trajectory_name)
       print (trajectory_name)
   else:
       run_name = getattr(config_earth, "run_name", "SHAB9")
       trajectory_name = config_earth.run_name+forecast_file_name

   # Variables for Simulation and Plotting
   T_s = [atm.T]
   T_i = [atm.T]
   T_atm = [atm.T]
   el =  [coord["alt"]]
   v= [0.]
   coords = [coord]

   x_winds_old = [0]
   y_winds_old = [0]
   x_winds_new = [0]
   y_winds_new = [0]

   ttt = [t - pd.Timedelta(hours=GMT)] #Just for visualizing plot better]
   data_loss = False
   burst = False
   gmap1 = gmplot.GoogleMapPlotter(coord["lat"],coord["lon"], 9, apikey=secret_keys.google_maps_api_key()) #9 is how zoomed in the map starts, the lower the number the more zoomed out

   e = solve_states.SolveStates()

   if forecast_type == "GFS":
       try:
           gfs = GFS.GFS(coord)
       except RuntimeError as e:
           print(colored(f"\n[FORECAST ERROR] {e}\n", "red"))
           raise
   else:
       gfs = ERA5.ERA5(coord)

   lat_aprs_gps = [coord["lat"]]
   lon_aprs_gps = [coord["lon"]]
   ttt_aprs = [t - pd.Timedelta(hours=GMT)]
   coords_aprs = [coord]


   same_ll_count = 0
   old_lat = 0.0
   old_lon = 0.0
   sunset_descent_active = False
   # Safe defaults in case OutOfBoundsError fires on first iteration
   x_wind_vel = 0.0
   y_wind_vel = 0.0
   x_wind_vel_old = 0.0
   y_wind_vel_old = 0.0
   bearing = 0.0

   # --- SIMULATION LOOP ---
   for i in range(0,sim_time):
       # STOP CHECK
       if stop_check_callback is not None and stop_check_callback():
           print(colored("Simulation stopped by user request. Saving partial data...", "red"))
           break

       # LANDED CHECK — stop before calling physics/GFS on a grounded balloon
       if el[i] <= min_alt:
           print(colored("Balloon has landed (alt=0.0 m). Ending simulation.", "cyan"))
           break

       if progress_callback is not None:
          progress_callback(i / max(1, sim_time - 1), f"Simulation step {i+1} of {sim_time}")

       T_s_new,T_i_new,T_atm_new,el_new,v_new, q_rad, q_surf, q_int = e.solveVerticalTrajectory(t,T_s[i],T_i[i],el[i],v[i],coord,alt_sp,v_sp)

       T_s.append(T_s_new)
       T_i.append(T_i_new)
       el.append(el_new)
       v.append(v_new)
       T_atm.append(T_atm_new)
       t = t + pd.Timedelta(hours=(1/3600*dt))
       ttt.append(t - pd.Timedelta(hours=GMT)) #Just for visualizing plot better

       if i % GFSrate == 0:
            try:
                lat_new,lon_new,x_wind_vel,y_wind_vel, x_wind_vel_old, y_wind_vel_old, bearing,nearest_lat, nearest_lon, nearest_alt = gfs.getNewCoord(coords[i],dt*GFSrate)
            except OutOfBoundsError as oob:
                print(colored(f"\n[OUT OF BOUNDS] Simulation stopped early:\n{oob}\n", "yellow"))
                # T_s, T_i, el, v, T_atm, t, ttt were already appended at top of loop.
                # Only the wind/coord arrays haven't been appended yet for this step.
                coords.append(coords[i]); lat.append(coords[i]["lat"]); lon.append(coords[i]["lon"])
                x_winds_old.append(x_wind_vel_old); y_winds_old.append(y_wind_vel_old)
                x_winds_new.append(x_wind_vel); y_winds_new.append(y_wind_vel)
                break
            print("lat: " + str(lat_new) +  " lon: "+ str(lon_new) + " alt: " + str(el_new) + " timestamp: " + str(t))
            if (lat_new == old_lat and lon_new == old_lat):
               same_ll_count = same_ll_count + 1
            if (same_ll_count > 5):
               break
       coord_new  =     {
                         "lat": lat_new,                # (deg) Latitude
                         "lon": lon_new,                # (deg) Longitude
                         "alt": el_new,                 # (m) Elevation
                         "timestamp": t,                # Timestamp
                       }

       print(str(coord_new))
       coords.append(coord_new)
       lat.append(lat_new)
       lon.append(lon_new)
       old_lat = lat_new
       old_lon = lon_new

       # Push live telemetry for real-time browser display
       if telemetry_callback is not None and i % GFSrate == 0:
           try:
               telemetry_callback({
                   "lat": float(lat_new),
                   "lon": float(lon_new),
                   "alt": float(el_new),
                   "time": t.strftime("%Y-%m-%dT%H:%M:%S"),
               })
           except Exception:
               pass

       x_winds_old.append(x_wind_vel_old)
       y_winds_old.append(y_wind_vel_old)
       x_winds_new.append(x_wind_vel)
       y_winds_new.append(y_wind_vel)

       rad = radiation.Radiation()
       zen = rad.get_zenith(t, coord_new)

       # ── Sunset descent: once sun is below horizon force 3 m/s descent ──────
       SUNSET_DESCENT_RATE = -3.0  # m/s
       if zen > math.pi / 2 and el_new > min_alt:
           if not sunset_descent_active:
               sunset_descent_active = True
               print(colored(f"[SUNSET] Sun below horizon at {t} (zen={math.degrees(zen):.1f}°). Beginning {abs(SUNSET_DESCENT_RATE)} m/s descent.", "yellow"))
           v_new = SUNSET_DESCENT_RATE
           el_new = max(min_alt, el[-1] + v_new * dt)
           # Replace the last appended values with the corrected ones
           el[-1] = el_new
           v[-1] = v_new
           if el_new <= min_alt:
               print(colored("Balloon landed after sunset descent. Ending simulation.", "cyan"))
               break

       if not (np.all(np.isfinite(el)) and np.all(np.isfinite(T_s)) and np.all(np.isfinite(T_i))):
            raise RuntimeError(f"State became non-finite: el={el}, T_s={T_s}, T_i={T_i}")


       if i % 360*(1/dt) == 0:
           print(str(t - pd.Timedelta(hours=GMT)) #Just for visualizing better
            +  " el " + str("{:.4f}".format(el_new))
            + " v " + str("{:.4f}".format(v_new))
            #+ " accel " + str("{:.4f}".format(dzdotdt))
            + " T_s " + str("{:.4f}".format(T_s_new))
            + " T_i " + str("{:.4f}".format(T_i_new))
            + " zen " + str(math.degrees(zen))
           )

           print(colored(("U wind speed: " + str(x_wind_vel) + " V wind speed: " + str(y_wind_vel) + " Bearing: " + str(bearing)),"yellow"))
           print(colored(("Lat: " + str(lat_new) + " Lon: " + str(lon_new) + " Nearest Lat: " + str(nearest_lat) + " Nearest Lon: " + str(nearest_lon) + " Nearest Alt: " + str(nearest_alt)),"cyan"))

   # --- POST SIMULATION FILE WRITING ---
   df = None
   #Plots
   #Output data to file

   iter = int(60/dt) #only output every minute - divide by # of seconds in dt
   df1 = pd.DataFrame(ttt[0::iter], columns = ['time (UTC)']) #from main_temp.py
   df2 = pd.DataFrame(lat[0::iter], columns = ['latitude']) #from main_temp.py
   df3 = pd.DataFrame(lon[0::iter], columns = ['longitude']) #from main_temp.py
   df4 = pd.DataFrame(el[0::iter], columns = ['elevation (m)']) #from main_temp.py
   df5 = pd.DataFrame(T_i[0::iter], columns = ['internal temperature (K)']) #from main_temp.py
   df6 = pd.DataFrame(T_s[0::iter], columns = ['surface temperature (K)']) #from main_temp.py
   df7 = pd.DataFrame(T_atm[0::iter], columns = ['atmospheric temperature (K)']) #from main_temp.py
   df8 = pd.DataFrame(x_winds_old[0::iter], columns = ['x wind velocity old (m/s)']) #from main_temp.py
   df9 = pd.DataFrame(y_winds_old[0::iter], columns = ['y wind velocity old (m/s)']) #from main_temp.py
   df10 = pd.DataFrame(x_winds_new[0::iter], columns = ['x wind velocity new (m/s)']) #from main_temp.py
   df11 = pd.DataFrame(y_winds_new[0::iter], columns = ['y wind velocity new (m/s)']) #from main_temp.py
   df_all = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11],axis=1)
   csvfn = "trajectories/" + trajectory_name +"_GFS_" + str(t.year) + "_" + str(t.month) + "_" + str(start.day) + '_' + curDate + '.csv'
   pd.DataFrame(df_all).to_csv(csvfn, index=False, header=True)

   import json as _json
   ic = {
       "run_name":       trajectory_name,
       "run_date":       curDate,
       "forecast_file":  config_earth.netcdf_gfs.get("nc_file", ""),
       "sim_start_time": start.strftime("%Y-%m-%dT%H:%M:%S"),
       "sim_time_hours": config_earth.simulation["sim_time"],
       "dt_seconds":     config_earth.simulation["dt"],
       "launch_lat":     config_earth.simulation["start_coord"]["lat"],
       "launch_lon":     config_earth.simulation["start_coord"]["lon"],
       "launch_alt_m":   config_earth.simulation["start_coord"]["alt"],
       "balloon_d_m":    config_earth.balloon_properties["d"],
       "payload_kg":     config_earth.balloon_properties["mp"],
       "envelope_kg":    config_earth.balloon_properties["mEnv"],
   }
   icfn = csvfn.replace('.csv', '_init.json')
   with open(icfn, 'w') as _f:
       _json.dump(ic, _f, indent=2, default=str)
   print(f"Initial conditions saved to {icfn}")

   def filter_latlon(lat, lon, threshold=0.00001):
       filtered_lat = [lat[0]]  # Start with the first element
       filtered_lon = [lon[0]]

       for i in range(1, len(lat)):
           # Check if the difference from the previous element is greater than the threshold
           if abs(lat[i] - lat[i-1]) >= threshold or abs(lon[i] - lon[i-1]) >= threshold:
               filtered_lat.append(lat[i])
               filtered_lon.append(lon[i])

       return filtered_lat, filtered_lon


   # Define the elevation to color function
   def elevation_to_color(altitude, maxAlt):
       """Returns a color string based on altitude."""
       if maxAlt == 0: maxAlt = 1 # avoid div by zero
       normalized_altitude = altitude / maxAlt
       red = int(255 * (normalized_altitude))
       green = int(255 * (1-normalized_altitude))
       return simplekml.Color.rgb(red, green, 0)

   # Haversine formula to calculate distance between two coordinates in meters
   def haversine(lon1, lat1, lon2, lat2):
       R = 6371000  # Radius of the Earth in meters
       phi1 = math.radians(lat1)
       phi2 = math.radians(lat2)
       delta_phi = math.radians(lat2 - lat1)
       delta_lambda = math.radians(lon2 - lon1)
       a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
       c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
       return R * c  # Output distance in meters

   # Check if the file exists
   if not os.path.exists(csvfn):
       raise FileNotFoundError(f"CSV file not found: {csvfn}")

   # Load CSV data
   dfKml = pd.read_csv(csvfn)

   # Ensure necessary columns exist
   required_columns = ['elevation (m)', 'longitude', 'latitude', 'time (UTC)']
   print(dfKml.columns)
   for col in required_columns:
       if col not in dfKml.columns:
           raise ValueError(f"Missing required column: {col}")

   # Create a KML object
   kml = simplekml.Kml()
   lineFolder = kml.newfolder(name='line')
   ptFolder = kml.newfolder(name='pts')

   # Initial parameters
   ptpa = previous_altitude = dfKml['elevation (m)'].iloc[0]
   previous_lon = dfKml['longitude'].iloc[0]
   previous_lat = dfKml['latitude'].iloc[0]
   previous_time = pd.to_datetime(dfKml['time (UTC)'].iloc[0])  # Convert time column to datetime
   maxAlt = dfKml['elevation (m)'].max()
   line_coordinates = []

   # Iterate through the rows in the DataFrame
   horizontal_distance_accum = 0  # Track horizontal distance to ensure a point every 1000m
   count = 0
   for index, row in dfKml.iterrows():
       if count % 10 != 0:
          count = count + 1
          continue
       count = count + 1
       current_altitude = row['elevation (m)']
       current_lon = row['longitude']
       current_lat = row['latitude']
       current_time = pd.to_datetime(row['time (UTC)'])
       ctz = current_time.strftime("%Y-%m-%dT%H:%M:%S")
       ptz = previous_time.strftime("%Y-%m-%dT%H:%M:%S")

       # Calculate horizontal distance and vertical speed
       horizontal_distance = haversine(previous_lon, previous_lat, current_lon, current_lat)
       time_diff = (current_time - previous_time).total_seconds()  # Time difference in seconds
       horizontal_speed = (horizontal_distance / 1000) / (time_diff / 3600) if time_diff > 0 else 0  # Speed in km/h
       vertical_speed = (current_altitude - previous_altitude) / time_diff if time_diff > 0 else 0  # Speed in m/s

       # Update cumulative horizontal distance
       horizontal_distance_accum += horizontal_distance

       # Check if 1000 meters of horizontal distance is reached or altitude changes significantly
       if horizontal_distance_accum >= 1000 or abs(current_altitude - ptpa) >= 10:
           # Create a KML point
           ptAlt = float(current_altitude/1000.0)
           point_name = f"{current_altitude/1000.0:.1f}km"
           point_desc = f"Lateral Speed: {horizontal_speed:.2f} kph\nVertical Speed: {vertical_speed:.2f} m/s\ncurrent_time {ctz}"
           pt = ptFolder.newpoint(name=point_name, coords=[(current_lon, current_lat, current_altitude)])
           pt.description = point_desc
           pt.timespan.begin = ptz
           pt.timespan.end = ctz
           pt.altitudemode = simplekml.AltitudeMode.absolute
           # Set point style
           style2 = simplekml.Style()
           style2.iconstyle.color = elevation_to_color(current_altitude, maxAlt)
           style2.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/target.png'
           style2.iconstyle.scale = 0.5
           pt.style = style2

           # Reset the horizontal distance tracker and update ptpa
           horizontal_distance_accum = 0
           ptpa = current_altitude

       # Check for linestring creation if altitude has changed by 10 meters or more
       if len(line_coordinates) > 3:
           # Create the linestring
           line_coordinates.append((current_lon, current_lat, current_altitude))
           linestring = lineFolder.newlinestring(coords=line_coordinates)
           linestring.altitudemode = simplekml.AltitudeMode.absolute
           linestring.style.linestyle.width = 5
           linestring.style.linestyle.color = elevation_to_color(previous_altitude, maxAlt)
           linestring.timespan.begin = ptz
           linestring.timespan.end = ctz
           line_coordinates = []  # Reset the coordinates for the new linestring
           line_coordinates.append((current_lon, current_lat, current_altitude))
           previous_altitude = current_altitude

       # Update for the next iteration
       line_coordinates.append((current_lon, current_lat, current_altitude))
       previous_lon, previous_lat, previous_time = current_lon, current_lat, current_time

   # Add the last linestring if there are remaining coordinates
   if line_coordinates:
       linestring = lineFolder.newlinestring(coords=line_coordinates)
       linestring.altitudemode = simplekml.AltitudeMode.absolute
       linestring.style.linestyle.width = 5
       linestring.style.linestyle.color = elevation_to_color(previous_altitude, maxAlt)

   # Save the KML file
   kmlfn = f"trajectories/{trajectory_name}_GFS_{t.year}_{t.month}_{start.day}_{curDate}.kml"
   kml.save(kmlfn)

   print(f"KML file '{kmlfn}' has been created successfully.")

   if balloon_trajectory != None:
       df = pd.read_csv(balloon_trajectory)
       df["time"] = pd.to_datetime(df['time'])
       df["time"] = df['time'] - pd.to_timedelta(7, unit='h') #Convert to MST
       df["dt"] = df["time"].diff().apply(lambda x: x/np.timedelta64(1, 's')).fillna(0).astype('int64')
       gmap1.plot(df['lat'], df['lng'],'white', edge_width = 2.5) # Actual Trajectory
       gmap1.text(coord["lat"]-.1, coord["lon"]-.2, trajectory_name + " True Trajectory", color='white')

   #Reforecasting
   if balloon_trajectory != None:
       alt_aprs = df["altitude"].to_numpy()
       time_aprs = df["time"].to_numpy()
       dt_aprs = df["dt"].to_numpy()
       t = config_earth.simulation['start_time']

       for i in range(0,len(alt_aprs)-1):

           lat_new,lon_new,x_wind_vel,y_wind_vel, x_wind_vel_old, y_wind_vel_old, bearing,nearest_lat, nearest_lon, nearest_alt = gfs.getNewCoord(coords_aprs[i],dt_aprs[i])

           t = t + pd.Timedelta(seconds=dt_aprs[i+1])
           ttt_aprs.append(t - pd.Timedelta(hours=GMT))


           coord_new  =	{
                             "lat": lat_new,                # (deg) Latitude
                             "lon": lon_new,                # (deg) Longitude
                             "alt": alt_aprs[i],                 # (m) Elevation
                             "timestamp": t,                # Timestamp
                           }

           print(ttt_aprs[i], dt_aprs[i])

           coords_aprs.append(coord_new)
           lat_aprs_gps.append(lat_new)
           lon_aprs_gps.append(lon_new)

           print(colored(("El: " + str(alt_aprs[i]) + " Lat: " + str(lat_new) + " Lon: " + str(lon_new) + " Bearing: " + str(bearing)),"green"))


   sns.set_palette("muted")
   fig, ax = plt.subplots()
   ax.plot(ttt,el, label = "reforecasted simulation")
   plt.xlabel('Datetime (MST)')
   plt.ylabel('Elevation (m)')
   if balloon_trajectory != None:
       ax.plot(df["time"],df["altitude"],label = "trajectory")

       if forecast_type == "GFS":
           gmap1.plot(lat_aprs_gps, lon_aprs_gps,'cyan', edge_width = 2.5) #Trajectory using Altitude balloon data with forecast data
           gmap1.text(coord["lat"]-.3, coord["lon"]-.2, trajectory_name + " Alt + " + forecast_type + " Wind Data" , color='cyan')
       elif forecast_type == "ERA5":
           gmap1.plot(lat_aprs_gps, lon_aprs_gps,'orange', edge_width = 2.5) #Trajectory using Altitude balloon data with forecast data
           gmap1.text(coord["lat"]-.3, coord["lon"]-.2, trajectory_name + " Alt + " + forecast_type + " Wind Data" , color='orange')

   # Trim all trajectory arrays to the same minimum length before plotting.
   # An early break (out-of-bounds, user stop, sunset) can leave arrays off by one.
   _n = min(len(ttt), len(T_s), len(T_i), len(T_atm), len(el), len(v),
            len(x_winds_old), len(y_winds_old), len(x_winds_new), len(y_winds_new))
   ttt        = ttt[:_n]
   T_s        = T_s[:_n]
   T_i        = T_i[:_n]
   T_atm      = T_atm[:_n]
   el         = el[:_n]
   v          = v[:_n]
   x_winds_old = x_winds_old[:_n]
   y_winds_old = y_winds_old[:_n]
   x_winds_new = x_winds_new[:_n]
   y_winds_new = y_winds_new[:_n]

   fig2, ax2 = plt.subplots()
   ax2.plot(ttt,T_s,label="Surface Temperature")
   ax2.plot(ttt,T_i,label="Internal Temperature")
   ax2.plot(ttt,T_atm,label="Atmospheric Temperature")
   plt.xlabel('Datetime (MST)')
   plt.ylabel('Temperature (K)')
   plt.legend(loc='upper right')
   plt.title('Solar Balloon Temperature - Earth')


   def windVectorToBearing(u, v):
       bearing = np.arctan2(v,u)
       speed = np.power((np.power(u,2)+np.power(v,2)),.5)
       return [bearing, speed]

   plt.legend(loc='upper right')
   plt.title('Wind Interpolation Comparison')

   #Winds Figure
   plt.figure()
   if any(x_winds_old):
       plt.plot(ttt, np.degrees(windVectorToBearing(x_winds_old, y_winds_old)[0]), label = "Bearing old", color = "blue")
   plt.plot(ttt, np.degrees(windVectorToBearing(x_winds_new, y_winds_new)[0]), label = "Bearing New", color = "red")
   plt.legend(loc='upper right')
   plt.title('Wind Interpolation Comparison')


   # Outline Downloaded NOAA forecast subset:

   if forecast_type == "GFS":
       region= zip(*[
           (gfs.LAT_LOW, gfs.LON_LOW),
           (gfs.LAT_HIGH, gfs.LON_LOW),
           (gfs.LAT_HIGH, gfs.LON_HIGH),
           (gfs.LAT_LOW, gfs.LON_HIGH)
       ])
       flat,flon=filter_latlon(lat,lon)
       gmap1.plot(flat, flon,'blue', edge_width = 2.5) # Simulated Trajectory
       gmap1.text(coord["lat"]-.2, coord["lon"]-.2, 'Simulated Trajectory with GFS Forecast', color='blue')
       gmap1.polygon(*region, color='cornflowerblue', edge_width=5, alpha= .2) #plot region

   elif forecast_type == "ERA5":
       region= zip(*[
           (gfs.LAT_LOW, gfs.LON_LOW),
           (gfs.LAT_HIGH, gfs.LON_LOW),
           (gfs.LAT_HIGH, gfs.LON_HIGH),
           (gfs.LAT_LOW, gfs.LON_HIGH)
       ])
       gmap1.plot(lat, lon,'red', edge_width = 2.5) # Simulated Trajectory
       gmap1.text(coord["lat"]-.2, coord["lon"]-.2, 'Simulated Trajectory with ERA5 Reanalysis', color='red')
       gmap1.polygon(*region, color='orange', edge_width=1, alpha= .15) #plot region


   year = str(tm.localtime()[0])
   month = str(tm.localtime()[1]).zfill(2)
   day = str(tm.localtime()[2]).zfill(2)

   if balloon_trajectory != None:
       if forecast_type == "GFS":
           gmap1.draw("trajectories/" + trajectory_name +"_GFS_" + str(t.year) + "_" + str(t.month) + "_" + str(start.day) + '_' + curDate + ".html" )

       elif forecast_type == "ERA5":
           gmap1.draw("trajectories/" + trajectory_name +"_ERA5_" + str(t.year) + "_" + str(t.month) + "_" + str(start.day) +'_' + curDate +  ".html" )
   else:
       if forecast_type == "GFS":
           gmap1.draw("trajectories/" + trajectory_name+"_PREDICTION_GFS_" + str(t.year) + "_" + str(t.month) + "_" + str(start.day) + '_' + curDate + ".html" )

       elif forecast_type == "ERA5":
           gmap1.draw("trajectories/PREDICTION_ERA5_" + str(t.year) + "_" + str(t.month) + "_" + str(start.day) + '_' + curDate + ".html" )


   executionTime = (tm.time() - scriptstartTime)
   print('\nSimulation executed in ' + str(executionTime) + ' seconds.')

   # ── Post-simulation plots and windmap ────────────────────────────────────
   # Wrapped in try/except so an early exit (out-of-bounds, sunset landing)
   # doesn't prevent the KML/CSV from being marked as complete.
   try:
       wm = windmap.Windmap()
       wm.plotWind2(wm.hour_index, wm.LAT, wm.LON)
       try:
           wm.file.close()
       except Exception:
           pass
   except Exception as wm_err:
       print(colored(f"[WARN] Windmap/plot skipped: {wm_err}", "yellow"))

   try:
       plt.show(block=False)
   except Exception:
       pass

   def multipage(filename, figs=None, dpi=200):
       pp = PdfPages(filename)
       if figs is None:
           figs = [plt.figure(n) for n in plt.get_fignums()]
       for fig in figs:
           fig.savefig(pp, format='pdf')
       pp.close()

   try:
       plt.pause(1)
   except Exception:
       pass

   pdf = "trajectories/" + trajectory_name + "_PREDICTION_GFS_" + str(t.year) + "_" + str(t.month) + "_" + str(start.day) + '_' + curDate + ".pdf"
   traj = "trajectories/" + trajectory_name+"_PREDICTION_GFS_" + str(t.year) + "_" + str(t.month) + "_" + str(start.day) + '_' + curDate + ".html"
   try:
       multipage(pdf)
   except Exception as pdf_err:
       print(colored(f"[WARN] PDF generation skipped: {pdf_err}", "yellow"))

   try:
       with zipfile.ZipFile("traj.zip", mode="w") as archive:
           archive.write(traj)
   except Exception:
       pass

   try:
       with open(traj, 'r') as file:
           yag = yagmail.SMTP(user=secret_keys.gmail_address(), password=secret_keys.google_app_password())
           yag.send(to=secret_keys.gmail_address(), subject='prediction ', contents=file, attachments=['traj.zip',kmlfn,csvfn])
           print("Email sent successfully")
   except Exception:
       print("Error, email was not sent")


if __name__ == "__main__":
    run_simulation()
