from datetime import datetime
import os
import glob

# ─────────────────────────────────────────────────────────────────────────────
# Run name
# ─────────────────────────────────────────────────────────────────────────────
run_name = 'SHAB11'   # (was: run_mane – typo fixed)

# ─────────────────────────────────────────────────────────────────────────────
# Balloon properties
# ─────────────────────────────────────────────────────────────────────────────
balloon_properties = dict(
    shape          = 'sphere',
    d              = 5.81,              # (m)       Diameter of Sphere Balloon
    mp             = 1.9,               # (kg)      Mass of Payload
    areaDensityEnv = 939. * 7.62E-6,    # (kg/m^2)  rhoEnv * envThickness
    mEnv           = 2.0,               # (kg)      Mass of Envelope
    cp             = 2000.,             # (J/kg·K)  Specific heat of envelope material
    absEnv         = .98,               # Absorptivity of envelope material
    emissEnv       = .95,               # Emissivity of envelope material
    Upsilon        = 4.5,               # Ascent resistance coefficient
)

# ─────────────────────────────────────────────────────────────────────────────
# GFS model-run auto-detection
# ─────────────────────────────────────────────────────────────────────────────
# Previously this block ran at module-import time and called the now-defunct
# NOAA DODS endpoint, crashing every import.  It has been moved into a
# function so it only runs when explicitly needed (i.e. inside autoNETCDF.py).
# The config still exposes the result through forecast_start_time below,
# but only after _detect_latest_gfs_run() is called.

def _detect_latest_gfs_run():
    """
    Query the live NOMADS production directory to find the latest available
    GFS 0.25-degree model run.

    Strategy
    --------
    1. Fetch the date-level listing from:
         https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/
       This page lists folders named  gfs.YYYYMMDD/  and is independent of
       the now-defunct DODS/OpenDAP service.
    2. Take the most recent date folder, then check which cycle subdirectories
       (00, 06, 12, 18) exist inside it by probing for the sentinel file
         gfs.tCCz.pgrb2.0p25.f000
       in the  gfs.YYYYMMDD/CC/atmos/  path.  The highest cycle that has this
       file is the latest complete run.
    3. If anything goes wrong (network error, unexpected page format, etc.)
       fall back to today's UTC date and the most recent 6-hourly cycle
       calculated from the current UTC time — so the fallback is always
       current rather than a hard-coded historical date.

    Returns
    -------
    (year, month, day, hourstamp) as zero-padded strings,
    e.g. ('2025', '02', '28', '12').
    """
    import requests
    from bs4 import BeautifulSoup
    from datetime import timezone

    PROD_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"
    CYCLES    = ['18', '12', '06', '00']   # newest first

    # ── Dynamic fallback: today's UTC date + most recent cycle ───────────────
    def _today_fallback():
        now   = datetime.now(tz=timezone.utc)
        cycle = str((now.hour // 6) * 6).zfill(2)
        fb    = (str(now.year), str(now.month).zfill(2),
                 str(now.day).zfill(2), cycle)
        print(f"[config_earth] Using fallback: {fb[0]}-{fb[1]}-{fb[2]} {fb[3]}Z")
        return fb

    try:
        # ── Step 1: get list of date folders ─────────────────────────────────
        resp = requests.get(PROD_BASE, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        # Folder names look like  gfs.20250228/
        import re
        date_folders = sorted(
            set(re.findall(r'gfs\.(\d{8})/', soup.get_text())),
            reverse=True   # newest first
        )

        if not date_folders:
            print("[config_earth] No GFS date folders found in NOMADS listing.")
            return _today_fallback()

        # ── Step 2: find the newest cycle that has its f000 file present ─────
        for date_str in date_folders[:3]:   # try the 3 most recent dates
            year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
            for cyc in CYCLES:
                probe_url = (
                    f"{PROD_BASE}gfs.{date_str}/{cyc}/atmos/"
                    f"gfs.t{cyc}z.pgrb2.0p25.f000"
                )
                try:
                    head = requests.head(probe_url, timeout=10)
                    if head.status_code == 200:
                        print(f"[config_earth] Latest GFS run: {date_str} {cyc}Z")
                        return (year, month, day, cyc)
                except requests.RequestException:
                    continue   # try the next cycle

        # Nothing found in the last 3 date folders
        print("[config_earth] Could not confirm any recent GFS run; using fallback.")
        return _today_fallback()

    except Exception as exc:
        print(f"[config_earth] GFS run detection failed ({exc}); using fallback.")
        return _today_fallback()


# ─────────────────────────────────────────────────────────────────────────────
# Forecast start time
# ─────────────────────────────────────────────────────────────────────────────
# Call the detection function once here so the rest of the config can use it.
# To hard-code a specific run instead, comment out the _detect call and
# uncomment one of the manual examples below.

_year, _month, _day, _hourstamp = _detect_latest_gfs_run()
forecast_start_time = f"{_year}-{_month}-{_day} {_hourstamp}:00:00"

# ── Manual overrides (uncomment one to use a specific flight) ─────────────────
#forecast_start_time = "2024-09-27 12:00:00"  # SHAB14-V
#forecast_start_time = "2022-04-09 12:00:00"  # SHAB10
#forecast_start_time = "2021-05-12 12:00:00"  # SHAB5
#forecast_start_time = "2020-11-20 06:00:00"  # SHAB3
#forecast_start_time = "2021-03-29 12:00:00"  # SHAB9
#forecast_start_time = "2023-04-18 00:00:00"  # Hawaii

# ─────────────────────────────────────────────────────────────────────────────
# Launch / simulation start time  (UTC)
# ─────────────────────────────────────────────────────────────────────────────
start_time = datetime.fromisoformat("2026-03-01 14:30:00")
balloon_trajectory = None

# ── Per-flight overrides ──────────────────────────────────────────────────────
#start_time         = datetime.fromisoformat("2024-09-27 14:00:00"); balloon_trajectory = None                             # SHAB14-V
#start_time         = datetime.fromisoformat("2022-04-09 18:14:00"); balloon_trajectory = "balloon_data/SHAB10V-APRS.csv"  # SHAB10
#start_time         = datetime.fromisoformat("2021-05-12 14:01:00"); balloon_trajectory = "balloon_data/SHAB5V_APRS_Processed.csv"  # SHAB5
#start_time         = datetime.fromisoformat("2020-11-20 15:47:00"); balloon_trajectory = "balloon_data/SHAB3V-APRS.csv"   # SHAB3
#start_time         = datetime.fromisoformat("2023-04-18 18:00:00"); balloon_trajectory = None                             # Hawaii

# ─────────────────────────────────────────────────────────────────────────────
# Forecast dict  (used by GFS.py and autoNETCDF.py)
# ─────────────────────────────────────────────────────────────────────────────
forecast = dict(
    forecast_type       = "GFS",                 # "GFS" or "ERA5"
    forecast_start_time = forecast_start_time,
    GFSrate             = 60,                    # (s) Wind lookup interval
)

# ─────────────────────────────────────────────────────────────────────────────
# NetCDF / GFS download parameters
# ─────────────────────────────────────────────────────────────────────────────
netcdf_gfs = dict(
    # DO NOT CHANGE – file path is derived from forecast start time
    nc_file   = (
        "forecasts/gfs_0p25_"
        + forecast['forecast_start_time'][0:4]
        + forecast['forecast_start_time'][5:7]
        + forecast['forecast_start_time'][8:10]
        + "_"
        + forecast['forecast_start_time'][11:13]
        + ".nc"
    ),
    nc_start  = datetime.fromisoformat(forecast['forecast_start_time']),
    hourstamp = forecast['forecast_start_time'][11:13],

    res           = 0.25,   # (deg) DO NOT CHANGE

    # Bounding-box half-widths in index counts (1 index = res degrees)
    lat_range     = 20,     # (.25 deg steps each side of centre)
    lon_range     = 60,     # (.25 deg steps each side of centre)
    download_days = 2,      # (1–10) days of forecast to download
)

# ─────────────────────────────────────────────────────────────────────────────
# ERA5 parameters
# ─────────────────────────────────────────────────────────────────────────────
netcdf_era5 = dict(
    #filename = "SHAB3V_era_20201120_20201121.nc",
    #filename = "SHAB5V-ERA5_20210512_20210513.nc",
    #filename = "shab10_era_2022-04-09to2022-04-10.nc",
    filename      = "SHAB14V_ERA5_20220822_20220823.nc",
    #filename     = "hawaii-ERA5-041823.nc",
    resolution_hr = 1,
)

# ─────────────────────────────────────────────────────────────────────────────
# Simulation parameters
# ─────────────────────────────────────────────────────────────────────────────
simulation = dict(
    start_time = start_time,
    sim_time   = 18,            # (hours) Duration to simulate

    vent    = 0.0,              # (kg/s) Vent mass flow rate
    alt_sp  = 15000.0,          # (m)    Altitude setpoint
    v_sp    = 0.,               # (m/s)  Vertical speed setpoint (not implemented)

    start_coord = {
        # ── Launch site options (uncomment one) ──────────────────────────────
        "lat": 35.19605,        # BFP
        "lon": -106.59733,      # BFP
        #"lat": 35.177864,      # DRMS
        #"lon": -106.547857,    # DRMS
        #"lat": 33.635050,      # CMS
        #"lon": -103.972350,    # CMS
        #"lat": 35.096440,      # AHS
        #"lon": -106.636764,    # AHS
        "alt"       : 1553.,    # (m)   Ground elevation
        "timestamp" : start_time,
    },

    min_alt           = 0.,     # (m) Minimum altitude (same as launch elevation)
    float             = 25000,  # (m) Float altitude for trapezoid.py
    dt                = 2.0,    # (s) Integration timestep

    balloon_trajectory = balloon_trajectory,
)

# ─────────────────────────────────────────────────────────────────────────────
# Earth / atmosphere constants
# ─────────────────────────────────────────────────────────────────────────────
earth_properties = dict(
    Cp_air0  = 1003.8,      # (J/kg·K) Specific heat, constant pressure
    Cv_air0  = 716.,        # (J/kg·K) Specific heat, constant volume
    Rsp_air  = 287.058,     # (J/kg·K) Specific gas constant for dry air
    P0       = 101325.0,    # (Pa)     Sea-level pressure
    emissGround = .95,      # Ground emissivity (assumption)
    albedo   = 0.17,        # Surface albedo (assumption)
)
