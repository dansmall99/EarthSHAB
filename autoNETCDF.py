# autoNETCDF.py
#
# EarthSHAB GFS forecast downloader – GRIB-filter edition
# --------------------------------------------------------
# NOAA retired the OpenDAP/DODS endpoint this file originally used.
# This replacement uses the NOAA GRIB-filter service instead:
#   https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl
#
# Auto-detection of the latest available model run is preserved: the script
# scrapes the NOMADS directory listing to find the newest GFS cycle, exactly
# as before, but then downloads GRIB2 files and converts them to NetCDF.
#
# New dependencies (add to requirements.txt):
#   cfgrib>=0.9.10
#   eccodes>=2.31      (C library – see README_GRIB_MIGRATION.md)
#   requests>=2.28     (already used by the original file)
#   beautifulsoup4     (already used by the original file)
#   xarray             (already in EarthSHAB requirements)


def generate_gfs_netcdf():
    import os
    import sys
    import time
    import tempfile
    import datetime

    import numpy as np
    import requests
    from termcolor import colored
    import xarray as xr
    import cfgrib
    import netCDF4 as nc4

    import config_earth

    # ── Config ────────────────────────────────────────────────────────────────
    # Use forecast_center if set separately, otherwise default to launch site
    coord         = config_earth.simulation.get("forecast_center") or config_earth.simulation['start_coord']
    lat_range     = config_earth.netcdf_gfs['lat_range']
    lon_range     = config_earth.netcdf_gfs['lon_range']
    download_days = config_earth.netcdf_gfs['download_days']
    res           = config_earth.netcdf_gfs['res']
    out_file      = config_earth.netcdf_gfs['nc_file']

    if not os.path.exists('forecasts'):
        os.makedirs('forecasts')

    # Seconds to wait between GRIB-filter fetches (NOAA requires >= 10 s)
    FETCH_DELAY = 2

    # Pressure levels available in GFS 0.25° pgrb2 isobaric data.
    # This is the exact set accepted by the GRIB filter — levels not in this
    # list (e.g. 875, 825, 775 mb) cause an "invalid parameter" error.
    # Source: https://nomads.ncep.noaa.gov/gribfilter.php?ds=gfs_0p25
    PRESSURE_LEVELS_MB = [
        1000, 975, 950, 925, 900, 850, 800, 750, 700, 650,
         600, 550, 500, 450, 400, 350, 300, 250, 200, 150,
         100,  70,  50,  40,  30,  20,  15,  10,
    ]

    FILTER_BASE = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"

    # ── Helpers (unchanged from original) ────────────────────────────────────
    def closest(arr, k):
        """Given an ordered array and a value, returns the index of the nearest item."""
        return min(range(len(arr)), key=lambda i: abs(arr[i] - k))

    def getNearestLat(lat, mn, mx):
        arr = np.arange(start=mn, stop=mx, step=res)
        return closest(arr, lat)

    def getNearestLon(lon, mn, mx):
        lon = lon % 360
        arr = np.arange(start=mn, stop=mx, step=res)
        return closest(arr, lon)

    lat_i = getNearestLat(coord["lat"], -90, 90.01)
    lon_i = getNearestLon(coord["lon"], 0, 360)

    # ── Auto-detect latest GFS model run ─────────────────────────────────────
    # Delegate entirely to config_earth._detect_latest_gfs_run(), which uses
    # the live NOMADS production file server instead of the defunct DODS endpoint.
    print("Detecting latest available GFS model run …")
    year, month, day, hourstamp = config_earth._detect_latest_gfs_run()

    date_str   = f"{year}{month}{day}"
    cycle_hour = int(hourstamp)

    print("Using GFS model run:")
    print(colored(f"  {date_str} {hourstamp}Z", "cyan"))

    # ── Build bounding box from lat/lon index offsets ─────────────────────────
    # Original code used index offsets (lat_range, lon_range as array index counts).
    # Convert back to degrees for the GRIB-filter URL.
    # NOTE: GRIB filter requires longitudes in [-180, 180], NOT [0, 360].
    # Round to 4 dp to prevent floating point garbage appearing in the URL.
    center_lat = coord["lat"]
    center_lon = coord["lon"]   # keep as-is; config stores as -180 to 180

    top    = round(min( 90.0, center_lat + lat_range * res), 4)
    bottom = round(max(-90.0, center_lat - lat_range * res), 4)
    left   = round(max(-180.0, center_lon - lon_range * res), 4)
    right  = round(min( 180.0, center_lon + lon_range * res), 4)

    # ── GRIB-filter URL builder ───────────────────────────────────────────────
    def _level_params():
        return "&".join(f"lev_{p}_mb=on" for p in PRESSURE_LEVELS_MB)

    def build_url(fhr):
        fhh = f"{int(fhr):03d}"
        cyc = f"{cycle_hour:02d}"
        return (
            f"{FILTER_BASE}"
            f"?file=gfs.t{cyc}z.pgrb2.0p25.f{fhh}"
            f"&{_level_params()}"
            f"&var_UGRD=on&var_VGRD=on&var_TMP=on&var_HGT=on"
            f"&subregion="
            f"&toplat={top}&leftlon={left}&rightlon={right}&bottomlat={bottom}"
            f"&dir=%2Fgfs.{date_str}%2F{cyc}%2Fatmos"
        )

    # ── Download GRIB2 files and convert ─────────────────────────────────────
    # Match original loop: range(0, download_days*8+1) steps of 3 hours each
    n_steps       = download_days * 8 + 1
    forecast_hours = [i * 3 for i in range(n_steps)]

    hourly_datasets = []

    with tempfile.TemporaryDirectory(prefix="earthshab_grib_") as tmpdir:
        for step_i, fhr in enumerate(forecast_hours):
            url      = build_url(fhr)
            grb_path = os.path.join(tmpdir, f"gfs_{date_str}_{cycle_hour:02d}z_f{fhr:03d}.grb2")

            print(f"  Downloading f{fhr:03d} (step {step_i}/{n_steps-1}) … ", end="", flush=True)

            # Download with retries
            ok = False
            for attempt in range(1, 4):
                try:
                    resp = requests.get(url, timeout=120, stream=True)
                    resp.raise_for_status()
                    if "html" in resp.headers.get("Content-Type", "").lower():
                        print("server returned HTML (bad request) – skipping")
                        break
                    with open(grb_path, "wb") as fh:
                        for chunk in resp.iter_content(chunk_size=1 << 16):
                            fh.write(chunk)
                    ok = True
                    break
                except requests.RequestException as exc:
                    print(f"\n    attempt {attempt}/3 failed: {exc}", end="")
                    if attempt < 3:
                        time.sleep(FETCH_DELAY * 2)

            if not ok:
                print(" FAILED – skipping")
                time.sleep(FETCH_DELAY)
                continue

            size_kb = os.path.getsize(grb_path) / 1024
            print(f"OK ({size_kb:.1f} kB)")

            # Open with cfgrib
            try:
                datasets = cfgrib.open_datasets(
                    grb_path,
                    backend_kwargs={"indexpath": ""},
                )
            except Exception as exc:
                print(f"    cfgrib error: {exc} – skipping")
                time.sleep(FETCH_DELAY)
                continue

            isobaric_ds = [ds for ds in datasets if "isobaricInhPa" in ds.coords]
            if not isobaric_ds:
                print(f"    No isobaric data – skipping")
                time.sleep(FETCH_DELAY)
                continue

            ds = xr.merge(isobaric_ds, compat="override")

            # Stamp with valid time
            run_dt     = datetime.datetime(int(year), int(month), int(day), cycle_hour)
            valid_time = run_dt + datetime.timedelta(hours=fhr)
            ds = ds.expand_dims("time").assign_coords(
                time=[np.datetime64(valid_time, "ns")]
            )
            hourly_datasets.append(ds)

            time.sleep(FETCH_DELAY)

    if not hourly_datasets:
        print(colored("No data downloaded. Check config and network.", "red"))
        sys.exit(1)

    # ── Merge all timesteps ───────────────────────────────────────────────────
    print("\nMerging all forecast hours …")
    combined = xr.concat(hourly_datasets, dim="time")

    # ── Rename to match EarthSHAB variable names ──────────────────────────────
    # cfgrib short names  →  EarthSHAB / original DODS names
    rename_map = {}
    name_mapping = {
        "u":              "ugrdprs",
        "v":              "vgrdprs",
        "t":              "tmpprs",
        "gh":             "hgtprs",
        "isobaricInhPa":  "lev",
        "latitude":       "lat",
        "longitude":      "lon",
    }
    for cfgrib_name, earthshab_name in name_mapping.items():
        if cfgrib_name in combined:
            rename_map[cfgrib_name] = earthshab_name
        if cfgrib_name in combined.coords or cfgrib_name in combined.dims:
            rename_map[cfgrib_name] = earthshab_name

    combined = combined.rename(rename_map)

    if "lev" in combined.coords:
        combined["lev"].attrs.update({"units": "hPa", "long_name": "pressure level"})

    # ── Write NetCDF ──────────────────────────────────────────────────────────
    print(f"Writing {out_file} …")
    encoding = {
        var: {"zlib": True, "complevel": 4}
        for var in combined.data_vars
    }
    combined.to_netcdf(out_file, encoding=encoding)

    print(colored(f"\nDone! Saved to: {out_file}", "green"))
    print(f"  Dimensions : {dict(combined.sizes)}")
    print(f"  Variables  : {list(combined.data_vars)}")
    return out_file


if __name__ == "__main__":
    print("Generating GFS NetCDF via autoNETCDF.py...")
    fn = generate_gfs_netcdf()
    print("Wrote:", fn)
