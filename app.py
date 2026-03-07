from flask import (
    Flask,
    request,
    render_template,
    jsonify,
    send_from_directory,
    url_for,
    redirect,
    session,
)
import threading
import os
import shutil
import csv
import glob
from datetime import datetime, timedelta
import io
from contextlib import redirect_stdout, redirect_stderr
import importlib
import time
import subprocess
import netCDF4
import config_earth
import main as earth_main
import shutil
import json
import uuid
import fcntl
import traceback
import re
import math
import sys
import numpy as np
import secret_keys

# ---------------------------------------------------------------------
# Persistence (Sticky Settings)
# ---------------------------------------------------------------------
SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "user_settings.json")

def load_user_settings():
    if not os.path.exists(SETTINGS_FILE):
        return {"visible_kmls": []}
    try:
        with open(SETTINGS_FILE, 'r') as f:
            data = json.load(f)
            if "visible_kmls" not in data: data["visible_kmls"] = []
            return data
    except Exception:
        return {"visible_kmls": []}

def save_user_settings(data, kml_list=None):
    keys_to_save = [
        "lat", "lon", "start_alt", "sim_time", "dt", "vent",
        "d", "mp", "mEnv", "run_name", "email_to", "netcdf_gfs_nc_file",
        "start_time", "lat_range", "lon_range", "forecast_center_lat", "forecast_center_lon"
    ]
    current = load_user_settings()

    for k in keys_to_save:
        if k in data:
            current[k] = data[k]

    if kml_list is not None:
        current["visible_kmls"] = kml_list

    try:
        with open(SETTINGS_FILE, 'w') as f:
            json.dump(current, f, indent=2)
    except Exception as e:
        print(f"Error saving settings: {e}")

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------

LOG_MAX_BYTES = 800_000  # Keep log field under ~800 KB so status file stays < 1 MB

def _tail_log(text, limit=LOG_MAX_BYTES):
    """Trim log to most recent `limit` bytes, always keeping complete lines."""
    if not text:
        return text
    if len(text) > limit:
        trimmed = text[-limit:]
        # Snap to the next newline so we don't start mid-line
        nl = trimmed.find("\n")
        if nl != -1:
            trimmed = trimmed[nl+1:]
        return "[...log trimmed...]\n" + trimmed
    return text

def clear_directory_contents(directory_path):
    abs_path = os.path.abspath(directory_path)
    if not os.path.exists(abs_path):
        return
    # Retry loop — a netCDF4 file may still be held open briefly after simulation
    for attempt in range(5):
        try:
            shutil.rmtree(directory_path)
            os.mkdir(directory_path)
            return
        except OSError as e:
            if e.errno == 16 and attempt < 4:  # EBUSY — wait and retry
                time.sleep(1)
                continue
            # Final fallback: delete files individually, skipping busy ones
            for fname in os.listdir(directory_path):
                fpath = os.path.join(directory_path, fname)
                try:
                    if os.path.isfile(fpath): os.remove(fpath)
                    elif os.path.isdir(fpath): shutil.rmtree(fpath)
                except OSError:
                    pass  # skip files still in use
            return

def emit_log(log_buf, progress=None, message=None, task=None, running=None, error=None):
    try:
        text = log_buf.getvalue()
    except Exception:
        text = str(log_buf)
    set_status(progress=progress, message=message, task=task, running=running, error=error, log=text)

# ---------------------------------------------------------------------
# App Setup
# ---------------------------------------------------------------------
app = Flask(__name__)
app.secret_key = secret_keys.get("FLASK_SECRET_KEY") or os.urandom(24)
app.permanent_session_lifetime = timedelta(days=30)

# ── Authentication ────────────────────────────────────────────────────────────
@app.before_request
def require_login():
    public = {'login', 'static'}
    if request.endpoint not in public and not session.get('logged_in'):
        return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        if (username == secret_keys.get("APP_USERNAME") and
                password == secret_keys.get("APP_PASSWORD")):
            session.permanent = True
            session['logged_in'] = True
            return redirect(url_for('index'))
        error = "Invalid username or password."
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

SIM_STATUS = {
    "progress": 0, "message": "idle", "running": False, "error": None, "log": "", "task": "idle",
}
STATUS_PATH = os.path.join(os.path.dirname(__file__), "sim_status.json")
STATUS_LOCK_PATH = os.path.join(os.path.dirname(__file__), "sim_status.lock")
SERVER_INSTANCE_ID = str(uuid.uuid4())[:8]
SERVER_BOOT_TIME = time.time()
DEFAULT_STATUS = {
    "progress": 0, "message": "idle", "running": False, "error": None, "log": "", "task": "idle",
}

# --- GLOBAL STOP EVENT ---
STOP_EVENT = threading.Event()

def _read_status_file():
    try:
        if os.path.exists(STATUS_PATH):
            with open(STATUS_PATH, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    merged = dict(DEFAULT_STATUS)
                    merged.update(data)
                    return merged
    except Exception:
        pass
    return dict(DEFAULT_STATUS)

def _write_status_file(state):
    try:
        tmp = STATUS_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, STATUS_PATH)
    except OSError:
        # Fallback: write directly if atomic rename fails (e.g. cross-device)
        try:
            with open(STATUS_PATH, "w") as f:
                json.dump(state, f)
                f.flush()
        except Exception:
            pass

class _FileLock:
    """Simple advisory file lock using fcntl. The lock file is created on entry
    and deleted on exit so it never accumulates content or leaks across restarts."""
    def __init__(self, path):
        self.path = path
        self.fp = None
    def __enter__(self):
        self.fp = open(self.path, "w")   # "w" truncates; file only holds the lock
        try: fcntl.flock(self.fp.fileno(), fcntl.LOCK_EX)
        except: pass
        return self
    def __exit__(self, exc_type, exc, tb):
        try: fcntl.flock(self.fp.fileno(), fcntl.LOCK_UN)
        except: pass
        try: self.fp.close()
        except: pass
        self.fp = None
        # Remove the lock file so stale lock files never accumulate
        try: os.remove(self.path)
        except OSError: pass

def set_status(progress=None, message=None, running=None, error=None, log=None, task=None, live_points=None):
    with _FileLock(STATUS_LOCK_PATH):
        state = _read_status_file()
        if progress is not None: state["progress"] = int(progress)
        if message is not None: state["message"] = str(message)
        if running is not None: state["running"] = bool(running)
        if error is not None: state["error"] = str(error) if error else None
        if log is not None: state["log"] = str(log)
        if task is not None: state["task"] = str(task)
        if live_points is not None: state["live_points"] = live_points
        _write_status_file(state)
    SIM_STATUS.update(state)

# ---------------------------------------------------------------------
# Forecast Helpers
# ---------------------------------------------------------------------
REQUIRED_GFS_VARS = ["ugrdprs", "vgrdprs", "hgtprs"]

def _forecast_time_to_str(ft):
    if isinstance(ft, datetime): return ft.strftime("%Y-%m-%d %H:%M:%S")
    return str(ft)

def expected_gfs_nc_path(forecast_start_time):
    fs = _forecast_time_to_str(forecast_start_time)
    if len(fs) < 13: return ""
    return f"forecasts/gfs_0p25_{fs[0:4]}{fs[5:7]}{fs[8:10]}_{fs[11:13]}.nc"

def is_valid_gfs_file(nc_path, start_time=None):
    if not nc_path or not os.path.exists(nc_path):
        return False
    try:
        ds = netCDF4.Dataset(nc_path, "r")
    except Exception:
        return False
    try:
        for v in REQUIRED_GFS_VARS:
            if v not in ds.variables:
                return False
        if start_time is not None:
            fc_start = _nc_start_from_filename(nc_path)
            if fc_start is not None:
                # Compute real end time from number of time steps (3-hourly)
                n_times = len(ds.variables["time"]) if "time" in ds.variables else 0
                if n_times > 0:
                    fc_end = fc_start + timedelta(hours=3 * (n_times - 1))
                else:
                    fc_end = fc_start + timedelta(days=4)  # fallback
                st = start_time.replace(tzinfo=None)
                if not (fc_start <= st <= fc_end):
                    return False
    finally:
        try: ds.close()
        except Exception: pass
    return True

def _floor_to_6h(dt: datetime) -> datetime:
    dt = dt.replace(minute=0, second=0, microsecond=0)
    return dt.replace(hour=(dt.hour // 6) * 6)

def list_local_forecasts(forecasts_dir="forecasts"):
    if not os.path.isdir(forecasts_dir): return []
    paths = [p for p in glob.glob(os.path.join(forecasts_dir, "*.nc")) if os.path.isfile(p)]
    paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return paths

def _decode_nc_times(time_var):
    """Decode a NetCDF time variable handling both standard (with units)
    and cfgrib-style (seconds-since-epoch, no units attribute) encoding."""
    from datetime import datetime as _dt, timedelta as _td
    raw = time_var[:]
    units = getattr(time_var, "units", None)
    if units:
        decoded = netCDF4.num2date(raw, units, getattr(time_var, "calendar", "standard"))
        return [t.replace(tzinfo=None) for t in decoded]
    else:
        # cfgrib writes int64 seconds since 1970-01-01 UTC, no units attr
        return [_dt(1970, 1, 1) + _td(seconds=int(v)) for v in raw]

def _nc_start_from_filename(nc_path: str):
    """Parse forecast start time directly from filename like gfs_0p25_20260302_12.nc
    Returns datetime or None."""
    import re as _re
    m = _re.search(r'gfs_0p25_(\d{8})_(\d{2})', os.path.basename(nc_path))
    if m:
        try:
            return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H")
        except Exception:
            pass
    return None

def _forecast_time_coverage_with_times(nc_path: str):
    if not nc_path or not os.path.exists(nc_path): return None
    try:
        ds = netCDF4.Dataset(nc_path, "r")
        try:
            if "time" not in ds.variables: return None
            times_naive = _decode_nc_times(ds.variables["time"])
            if not times_naive: return None
            return {"start": times_naive[0], "end": times_naive[-1], "times": times_naive}
        finally: ds.close()
    except Exception: return None

def forecast_bounds(nc_path: str):
    if not nc_path or not os.path.exists(nc_path): return None
    try:
        ds = netCDF4.Dataset(nc_path, "r")
        try:
            lat_name = "lat" if "lat" in ds.variables else ("latitude" if "latitude" in ds.variables else None)
            lon_name = "lon" if "lon" in ds.variables else ("longitude" if "longitude" in ds.variables else None)
            if not lat_name or not lon_name: return None

            lat = ds.variables[lat_name][:]
            lon = ds.variables[lon_name][:]

            latv = np.asarray(lat).astype(float).ravel()
            lonv = np.asarray(lon).astype(float).ravel()

            if latv.size == 0 or lonv.size == 0: return None

            return dict(
                north=float(np.nanmax(latv)),
                south=float(np.nanmin(latv)),
                east=float(np.nanmax(lonv)),
                west=float(np.nanmin(lonv)),
            )
        finally:
            try: ds.close()
            except Exception: pass
    except Exception: return None

def suggested_start_time_after_1430_utc(nc_path: str):
    cov = _forecast_time_coverage_with_times(nc_path)
    if not cov: return None
    times = cov.get("times") or []
    if not times: return None
    t0 = times[0]
    threshold = datetime(t0.year, t0.month, t0.day, 14, 30)
    if t0 > threshold: threshold = t0
    for t in times:
        if t >= threshold: return t.strftime("%Y-%m-%dT%H:%M")
    return times[0].strftime("%Y-%m-%dT%H:%M")

def ensure_forecast_for_sim_start(form_data: dict, log_buf: io.StringIO) -> str:
    sim_start = config_earth.simulation["start_time"]
    optimal_forecast_start = _floor_to_6h(sim_start)
    nc_from_form = (form_data.get("netcdf_gfs_nc_file") or "").strip()

    if nc_from_form:
        nc_path = nc_from_form.replace("\\", "/")
        if os.path.exists(nc_path) and is_valid_gfs_file(nc_path, start_time=sim_start):
            fc_start = _nc_start_from_filename(nc_path)
            if fc_start is None:
                cov = _forecast_time_coverage_with_times(nc_path)
                fc_start = cov["start"] if (cov and cov.get("start")) else None
            if fc_start is not None:
                config_earth.forecast["forecast_type"] = "GFS"
                config_earth.forecast_start_time = fc_start.strftime("%Y-%m-%d %H:%M:%S")
                config_earth.forecast["forecast_start_time"] = config_earth.forecast_start_time
                config_earth.netcdf_gfs["nc_start"]  = fc_start
                config_earth.netcdf_gfs["hourstamp"] = fc_start.strftime("%H")
            log_buf.write(f"[FORECAST] Using {nc_path} (start={fc_start})\n")
            config_earth.netcdf_gfs["nc_file"] = nc_path
            return nc_path
        else:
            log_buf.write(f"[WARN] Selected file {nc_from_form} invalid for sim start. Switching to auto-download.\n")

    config_earth.forecast["forecast_type"] = "GFS"
    config_earth.forecast_start_time = optimal_forecast_start.strftime("%Y-%m-%d %H:%M:%S")
    config_earth.forecast["forecast_start_time"] = config_earth.forecast_start_time
    expected_path = expected_gfs_nc_path(optimal_forecast_start)
    config_earth.netcdf_gfs["nc_file"] = expected_path

    if os.path.exists(expected_path) and is_valid_gfs_file(expected_path, start_time=sim_start):
        config_earth.netcdf_gfs["nc_start"]  = optimal_forecast_start
        config_earth.netcdf_gfs["hourstamp"] = optimal_forecast_start.strftime("%H")
        # Warn if existing file may not cover the full sim duration
        sim_time_hours = config_earth.simulation.get("sim_time", 18)
        n_times = 0
        try:
            import netCDF4 as _nc4
            ds = _nc4.Dataset(expected_path, "r")
            n_times = len(ds.variables["time"]) if "time" in ds.variables else 0
            ds.close()
        except Exception: pass
        if n_times > 0:
            fc_end = optimal_forecast_start + timedelta(hours=3 * (n_times - 1))
            sim_end = sim_start + timedelta(hours=sim_time_hours)
            if sim_end > fc_end:
                log_buf.write(f"[WARN] Cached file only covers to {fc_end}, sim needs to {sim_end}. Consider re-downloading.\n")
            else:
                log_buf.write(f"[FORECAST] Using cached {expected_path} (covers to {fc_end})\n")
        else:
            log_buf.write(f"[FORECAST] Using cached {expected_path}\n")
        return expected_path

    if os.path.isfile(expected_path):
        try: os.remove(expected_path)
        except Exception: pass

    emit_log(log_buf, progress=10, message=f"Downloading forecast for {optimal_forecast_start}...", task="download", running=True)
    clear_directory_contents("forecasts")

    # ── Detect the latest available GFS run ──────────────────────────────────
    GFS_MAX_FORECAST_DAYS = 16
    GFS_MAX_FORECAST_HOURS = GFS_MAX_FORECAST_DAYS * 24

    import config_earth as _ce
    _yr, _mo, _dy, _hr = _ce._detect_latest_gfs_run()
    latest_gfs_run = datetime(int(_yr), int(_mo), int(_dy), int(_hr))
    latest_gfs_end = latest_gfs_run + timedelta(hours=GFS_MAX_FORECAST_HOURS)
    log_buf.write(f"[FORECAST] Latest GFS run: {latest_gfs_run}  covers to: {latest_gfs_end}\n")

    # Reject if sim_start is beyond what GFS can ever cover
    sim_time_hours = config_earth.simulation.get("sim_time", 18)
    sim_end = sim_start + timedelta(hours=sim_time_hours)
    if sim_start >= latest_gfs_end:
        raise RuntimeError(
            f"Simulation start ({sim_start}) is beyond the maximum GFS forecast horizon "
            f"({latest_gfs_end}, {GFS_MAX_FORECAST_DAYS} days from latest run {latest_gfs_run}). "
            f"Choose a simulation start time before {latest_gfs_end}."
        )
    if sim_end > latest_gfs_end:
        max_sim = int((latest_gfs_end - sim_start).total_seconds() / 3600)
        raise RuntimeError(
            f"Simulation end ({sim_end}) exceeds maximum GFS forecast horizon ({latest_gfs_end}). "
            f"Reduce sim_time to {max_sim}h or less for this start time."
        )

    # ── Compute download_days from latest_gfs_run to sim_end ─────────────────
    hours_needed = (sim_end - latest_gfs_run).total_seconds() / 3600
    days_needed = math.ceil(hours_needed / 24)
    days_needed = max(1, min(days_needed, GFS_MAX_FORECAST_DAYS))
    current_days = config_earth.netcdf_gfs.get("download_days", 3)
    if days_needed != current_days:
        log_buf.write(f"[FORECAST] Setting download_days={days_needed} to cover sim window "
                      f"({sim_start} + {sim_time_hours}h, needs {hours_needed:.0f}h from GFS run {latest_gfs_run})\n")
        config_earth.netcdf_gfs["download_days"] = days_needed
    log_buf.write(f"[FORECAST] Downloading {days_needed} days ({days_needed*8} steps x 3h)\n")

    import autoNETCDF
    err_holder = {"err": None}
    def _run_autonetcdf():
        try:
            with redirect_stdout(log_buf), redirect_stderr(log_buf):
                mod = importlib.reload(autoNETCDF)
                if hasattr(mod, "generate_gfs_netcdf"): mod.generate_gfs_netcdf()
        except Exception as e: err_holder["err"] = e

    t = threading.Thread(target=_run_autonetcdf, daemon=True)
    t.start()

    last_size = None
    last_change = time.time()
    while True:
        if os.path.exists(expected_path):
            try: sz = os.path.getsize(expected_path)
            except: sz = None
            if sz != last_size:
                last_size = sz
                last_change = time.time()
        emit_log(log_buf, progress=25, message="Downloading...", task="download", running=True)
        if err_holder["err"]: break
        if not t.is_alive() and (time.time() - last_change > 5): break
        time.sleep(2.0)

    t.join(timeout=5)
    if err_holder["err"]: raise RuntimeError(f"autoNETCDF error: {err_holder['err']}")

    # autoNETCDF names the file after the actual GFS run cycle it found,
    # which may differ from expected_path (based on sim start floor-to-6h).
    # Find the most recently modified .nc in forecasts/ as the actual file.
    actual_path = expected_path
    try:
        candidates = sorted(
            [os.path.join("forecasts", f) for f in os.listdir("forecasts") if f.endswith(".nc")],
            key=os.path.getmtime, reverse=True
        )
        if candidates:
            actual_path = candidates[0]
            if actual_path != expected_path:
                log_buf.write(f"[FORECAST] Actual file: {actual_path} (expected {expected_path})\n")
                config_earth.netcdf_gfs["nc_file"] = actual_path
                fc_start = _nc_start_from_filename(actual_path)
                if fc_start:
                    config_earth.netcdf_gfs["nc_start"]  = fc_start
                    config_earth.netcdf_gfs["hourstamp"] = fc_start.strftime("%H")
                    config_earth.forecast_start_time = fc_start.strftime("%Y-%m-%d %H:%M:%S")
                    config_earth.forecast["forecast_start_time"] = config_earth.forecast_start_time
    except Exception as fe:
        log_buf.write(f"[WARN] Could not find actual forecast file: {fe}\n")

    set_status(progress=18, message="Validating...", task="simulation", running=True)
    if not is_valid_gfs_file(actual_path, start_time=sim_start):
        raise RuntimeError(f"Downloaded forecast {actual_path} invalid/missing vars/coverage.")

    log_buf.write(f"[FORECAST] Download complete: {actual_path}\n")
    return actual_path

# ---------------------------------------------------------------------
# Trajectories Helpers
# ---------------------------------------------------------------------
def list_trajectory_files_data():
    base_dir = "trajectories"
    if not os.path.isdir(base_dir): return []
    items = []
    for fname in os.listdir(base_dir):
        path = os.path.join(base_dir, fname)
        if not os.path.isfile(path): continue
        root, ext = os.path.splitext(fname)
        ext = ext.lower().replace('.', '')
        if ext not in ["html", "pdf", "kml", "csv"]: continue
        try: mtime = os.path.getmtime(path)
        except OSError: mtime = 0.0
        items.append({
            "filename": fname,
            "ext": ext,
            "mtime": mtime,
            "time_str": datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S"),
        })
    items.sort(key=lambda x: x["mtime"], reverse=True)
    return items

def get_last_landing_from_latest_csv():
    base_dir = "trajectories"
    if not os.path.isdir(base_dir): return None
    latest_csv = None
    latest_mtime = -1.0
    for fname in os.listdir(base_dir):
        if not fname.lower().endswith(".csv"): continue
        path = os.path.join(base_dir, fname)
        mtime = os.path.getmtime(path)
        if mtime > latest_mtime:
            latest_mtime = mtime
            latest_csv = path
    if latest_csv is None: return None
    try:
        with open(latest_csv, newline="") as f:
            reader = csv.DictReader(f)
            last_row = None
            for row in reader: last_row = row
        if not last_row: return None
        lat_key = next((k for k in last_row.keys() if "lat" in k.lower()), None)
        lon_key = next((k for k in last_row.keys() if "lon" in k.lower() or "lng" in k.lower()), None)
        if lat_key and lon_key: return (last_row[lat_key], last_row[lon_key])
    except Exception: pass
    return None

# ---------------------------------------------------------------------
# Context & Routes
# ---------------------------------------------------------------------
def _get_current_context():
    sim = config_earth.simulation
    forecast = config_earth.forecast
    balloon = config_earth.balloon_properties
    saved_settings = load_user_settings()

    if "lat" in saved_settings: sim["start_coord"]["lat"] = float(saved_settings["lat"])
    if "lon" in saved_settings: sim["start_coord"]["lon"] = float(saved_settings["lon"])
    if "start_alt" in saved_settings: sim["start_coord"]["alt"] = float(saved_settings["start_alt"])
    if "sim_time" in saved_settings: sim["sim_time"] = int(saved_settings["sim_time"])
    if "dt" in saved_settings: sim["dt"] = float(saved_settings["dt"])
    if "vent" in saved_settings: sim["vent"] = float(saved_settings["vent"])
    if "d" in saved_settings: balloon["d"] = float(saved_settings["d"])
    if "mp" in saved_settings: balloon["mp"] = float(saved_settings["mp"])
    if "mEnv" in saved_settings: balloon["mEnv"] = float(saved_settings["mEnv"])
    if "lat_range" in saved_settings: config_earth.netcdf_gfs["lat_range"] = int(float(saved_settings["lat_range"]))
    if "lon_range" in saved_settings: config_earth.netcdf_gfs["lon_range"] = int(float(saved_settings["lon_range"]))
    if "forecast_center_lat" in saved_settings and "forecast_center_lon" in saved_settings:
        config_earth.simulation["forecast_center"] = {"lat": float(saved_settings["forecast_center_lat"]), "lon": float(saved_settings["forecast_center_lon"])}
    if "run_name" in saved_settings: config_earth.run_name = saved_settings["run_name"]
    if "email_to" in saved_settings: config_earth.email_to = saved_settings["email_to"]

    sess_start = session.get("sticky_start_time")
    if sess_start:
        try: sim["start_time"] = datetime.fromisoformat(sess_start)
        except: pass
    elif "start_time" in saved_settings:
        try: sim["start_time"] = datetime.fromisoformat(saved_settings["start_time"])
        except: pass

    if "netcdf_gfs_nc_file" in saved_settings:
         config_earth.netcdf_gfs["nc_file"] = saved_settings["netcdf_gfs_nc_file"]

    now_utc = datetime.utcnow().replace(second=0, microsecond=0)
    local_paths = list_local_forecasts("forecasts")
    local_files = [{"name": os.path.basename(p), "path": p} for p in local_paths]
    nc_path = (config_earth.netcdf_gfs.get("nc_file", "") or "").strip()

    if not nc_path and local_paths:
        nc_path = local_paths[0]
        config_earth.netcdf_gfs["nc_file"] = nc_path

    cov = _forecast_time_coverage_with_times(nc_path)
    cov_start = cov["start"].strftime("%Y-%m-%d %H:%M") if cov else ""
    cov_end = cov["end"].strftime("%Y-%m-%d %H:%M") if cov else ""
    landing = get_last_landing_from_latest_csv()

    return dict(
        forecast_start_time_local=forecast.get("forecast_start_time", "").replace(" ", "T"),
        netcdf_gfs_nc_file=nc_path,
        local_forecast_files=local_files,
        coverage_start=cov_start, coverage_end=cov_end,
        start_time_local=sim["start_time"].strftime("%Y-%m-%dT%H:%M") if isinstance(sim["start_time"], datetime) else "",
        now_utc_local=now_utc.strftime("%Y-%m-%dT%H:%M"),
        sim_time=sim["sim_time"], vent=sim["vent"], dt=sim["dt"],
        lat=sim["start_coord"]["lat"], lon=sim["start_coord"]["lon"], start_alt=sim["start_coord"]["alt"],
        d=balloon["d"], mp=balloon["mp"], mEnv=balloon["mEnv"],
        lat_range=config_earth.netcdf_gfs.get("lat_range", 20),
        lon_range=config_earth.netcdf_gfs.get("lon_range", 60),
        forecast_center_lat=config_earth.simulation.get("forecast_center", {}).get("lat", config_earth.simulation["start_coord"]["lat"]),
        forecast_center_lon=config_earth.simulation.get("forecast_center", {}).get("lon", config_earth.simulation["start_coord"]["lon"]),
        run_name=getattr(config_earth, "run_name", "SHAB9"),
        email_to=getattr(config_earth, "email_to", ""),
        trajectory_files=list_trajectory_files_data(),
        landing_lat=landing[0] if landing else None,
        landing_lon=landing[1] if landing else None,
        visible_kmls=saved_settings.get("visible_kmls", []),
        cesium_token=secret_keys.cesium_token()
    )

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html", **_get_current_context())

@app.route("/api/list_trajectory_files")
def api_list_files():
    """Returns JSON list of files for AJAX update"""
    return jsonify({"files": list_trajectory_files_data()})

@app.route("/forecast_info", methods=["GET"])
def forecast_info():
    path = request.args.get("path", "").strip()
    if not os.path.exists(path): return jsonify(ok=False)
    cov = _forecast_time_coverage_with_times(path)
    bounds = forecast_bounds(path)
    sugg = suggested_start_time_after_1430_utc(path)
    return jsonify(ok=True, bounds=bounds, suggested_start_time_local=sugg,
                   coverage_start=cov["start"].strftime("%Y-%m-%d %H:%M") if cov else "",
                   coverage_end=cov["end"].strftime("%Y-%m-%d %H:%M") if cov else "")

@app.route("/api/get_forecast_bounds", methods=["POST"])
def get_forecast_bounds():
    data = request.json
    sel_file = data.get('selected_file')
    nc_path = sel_file if sel_file else config_earth.netcdf_gfs.get("nc_file", "")
    if nc_path and os.path.exists(nc_path):
        bounds = forecast_bounds(nc_path)
        return jsonify(ok=True, bounds=bounds)
    return jsonify(ok=False)

@app.route("/api/remove_forecast", methods=["POST"])
def remove_forecast():
    clear_directory_contents("forecasts")
    return jsonify(ok=True)

CONSOLE_LOG_LIMIT = 800_000  # chars trimmed only for browser display

@app.route("/status", methods=["GET"])
def sim_status():
    s = _read_status_file()
    # Trim log only for the browser response; the status file stays full
    raw_log = s.get("log", "")
    if len(raw_log) > CONSOLE_LOG_LIMIT:
        trimmed = raw_log[-CONSOLE_LOG_LIMIT:]
        nl = trimmed.find("\n")
        s["log"] = ("[...trimmed...]\n" + trimmed[nl+1:]) if nl != -1 else trimmed
    s.update({"pid": os.getpid(), "instance": SERVER_INSTANCE_ID})
    return jsonify(s)

@app.route("/stop_simulation", methods=["POST"])
def stop_simulation():
    STOP_EVENT.set()
    # Give the worker a moment to notice the stop event
    time.sleep(0.3)
    # Hard-reset status to idle so any browser rejoining sees a clean state
    with _FileLock(STATUS_LOCK_PATH):
        _write_status_file(dict(DEFAULT_STATUS))
    SIM_STATUS.update(dict(DEFAULT_STATUS))
    return jsonify(ok=True, message="Simulation stopped and status reset.")


@app.route("/api/forecast_dirty", methods=["POST"])
def forecast_dirty():
    """Check whether the existing forecast file covers the requested center+bounds.
    Returns {dirty: true/false, reason: str}."""
    data = request.get_json(force=True) or {}
    center_lat = float(data.get("center_lat", 0))
    center_lon = float(data.get("center_lon", 0))
    lat_range  = int(data.get("lat_range", 20))
    lon_range  = int(data.get("lon_range", 60))
    res        = config_earth.netcdf_gfs.get("res", 0.25)

    nc_path = (config_earth.netcdf_gfs.get("nc_file") or "").strip()
    if not nc_path or not os.path.exists(nc_path):
        return jsonify(dirty=True, reason="No forecast file downloaded yet.")

    # Desired bounds
    want_n = center_lat + lat_range * res
    want_s = center_lat - lat_range * res
    want_e = center_lon + lon_range * res
    want_w = center_lon - lon_range * res

    try:
        import netCDF4 as _nc4
        ds = _nc4.Dataset(nc_path, "r")
        lats = ds.variables["lat"][:]
        lons = ds.variables["lon"][:]
        ds.close()
        have_n, have_s = float(lats.max()), float(lats.min())
        have_e, have_w = float(lons.max()), float(lons.min())
        # Convert stored lons (0-360) to -180-180 for comparison if needed
        if have_w > 180: have_w -= 360
        if have_e > 180: have_e -= 360
        want_e_adj = want_e if want_e <= 180 else want_e - 360
        want_w_adj = want_w if want_w >= -180 else want_w + 360
        fits = (have_s <= want_s and have_n >= want_n and
                have_w <= want_w_adj and have_e >= want_e_adj)
        if fits:
            return jsonify(dirty=False, reason="")
        else:
            return jsonify(dirty=True,
                reason=f"Current file covers lat [{have_s:.1f},{have_n:.1f}] "
                       f"lon [{have_w:.1f},{have_e:.1f}] but requested "
                       f"lat [{want_s:.1f},{want_n:.1f}] lon [{want_w:.1f},{want_e:.1f}].")
    except Exception as e:
        return jsonify(dirty=True, reason=str(e))

@app.route("/run_simulation", methods=["POST"])
def run_simulation():
    if SIM_STATUS.get("running"): return jsonify(started=False, error="Running"), 400
    try:
        form = request.form.to_dict()
        visible_kmls = request.form.getlist("visible_kmls")
        save_user_settings(form, kml_list=visible_kmls)

        if form.get("run_name"): session["sticky_run_name"] = form["run_name"]
        if form.get("start_time"): session["sticky_start_time"] = form["start_time"]

        def worker(fd):
            STOP_EVENT.clear()
            log_buf = io.StringIO()
            try:
                set_status(progress=5, message="Configuring...", running=True, task="simulation", log="")
                cfg = config_earth
                if fd.get("start_time"):
                    try:
                        dt_start = datetime.fromisoformat(fd["start_time"])
                        cfg.simulation["start_time"] = dt_start
                        cfg.start_time = dt_start
                        if "start_coord" in cfg.simulation:
                            cfg.simulation["start_coord"]["timestamp"] = dt_start
                    except ValueError: pass

                if fd.get("sim_time"): cfg.simulation["sim_time"] = int(fd["sim_time"])
                if fd.get("dt"): cfg.simulation["dt"] = float(fd["dt"])
                if fd.get("vent"): cfg.simulation["vent"] = float(fd["vent"])
                if fd.get("lat"): cfg.simulation["start_coord"]["lat"] = float(fd["lat"])
                if fd.get("lon"): cfg.simulation["start_coord"]["lon"] = float(fd["lon"])
                if fd.get("start_alt"): cfg.simulation["start_coord"]["alt"] = float(fd["start_alt"])
                if fd.get("lat_range"): cfg.netcdf_gfs["lat_range"] = int(float(fd["lat_range"]))
                if fd.get("lon_range"): cfg.netcdf_gfs["lon_range"] = int(float(fd["lon_range"]))
                # Forecast download center — defaults to launch site if not set separately
                fc_lat = float(fd["forecast_center_lat"]) if fd.get("forecast_center_lat") else cfg.simulation["start_coord"]["lat"]
                fc_lon = float(fd["forecast_center_lon"]) if fd.get("forecast_center_lon") else cfg.simulation["start_coord"]["lon"]
                cfg.simulation["forecast_center"] = {"lat": fc_lat, "lon": fc_lon}
                cfg.balloon_properties["d"] = float(fd.get("d") or 10)
                cfg.balloon_properties["mp"] = float(fd.get("mp") or 5)
                cfg.balloon_properties["mEnv"] = float(fd.get("mEnv") or 5)
                cfg.run_name = fd.get("run_name", "SHAB")
                cfg.email_to = fd.get("email_to", "")

                importlib.reload(earth_main)
                actual_nc = ensure_forecast_for_sim_start(fd, log_buf)
                # Guarantee config reflects the actual file on disk regardless of
                # any intermediate path that was set during the download process.
                config_earth.netcdf_gfs["nc_file"] = actual_nc
                # Also store in a separate key that windmap/GFS can rely on
                # even if nc_file gets mutated later by config parsing.
                config_earth.netcdf_gfs["actual_nc_file"] = actual_nc
                log_buf.write(f"[FORECAST] nc_file confirmed: {actual_nc}\n")

                def progress_cb(frac, msg=None):
                    try:
                        set_status(progress=20+int(75*frac), message=msg or "Simulating...",
                                   running=True, task="simulation",
                                   log=log_buf.getvalue())
                    except Exception:
                        pass

                def should_stop():
                    return STOP_EVENT.is_set()

                # Accumulate live telemetry points separately from the log
                # so they are never trimmed and all browsers can replay them.
                _live_points_buf = []
                def telemetry_cb(point):
                    _live_points_buf.append(point)
                    # Push to status file every 10 points to avoid hammering disk
                    if len(_live_points_buf) % 10 == 0:
                        set_status(live_points=list(_live_points_buf))

                set_status(progress=20, message="Simulating...", running=True, live_points=[])
                log_buf.write(f"\n[WORKER STARTED] PID: {os.getpid()}\n")
                try:
                    with redirect_stdout(log_buf), redirect_stderr(log_buf):
                         earth_main.run_simulation(progress_callback=progress_cb,
                                                   stop_check_callback=should_stop,
                                                   telemetry_callback=telemetry_cb)
                    # Final flush of any remaining points
                    if _live_points_buf:
                        set_status(live_points=list(_live_points_buf))
                except Exception as sim_err:
                     tb = traceback.format_exc()
                     log_buf.write(f"\n[CRITICAL SIMULATION ERROR]\n{tb}\n")
                     raise sim_err

                set_status(progress=100, message="Done", running=False, task="simulation", log=log_buf.getvalue())

            except Exception as e:
                tb = traceback.format_exc()
                import os as _os; _os.write(2, f"\n[WORKER ERROR] {tb}\n".encode())
                full_log = log_buf.getvalue() + f"\n[CRITICAL ERROR]\n{tb}\n"
                set_status(progress=100, message="Error", running=False, error=tb, log=full_log)

        t = threading.Thread(target=worker, args=(form,), daemon=True)
        t.start()
        return jsonify(started=True)
    except Exception as e: return jsonify(started=False, error=str(e))

@app.route("/latest_trajectory_json")
def latest_trajectory_json():
    # --- CHECK 1: SIMULATED (LIVE POINTS) ---
    if request.args.get('simulated'):
        state = _read_status_file()
        raw_points = state.get("live_points", [])
        if len(raw_points) > 3000:
            step = max(1, len(raw_points) // 3000)
            raw_points = raw_points[::step]
        resp = jsonify(points=raw_points, source="live_points",
                       running=state.get("running", False))
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        return resp

    # --- CHECK 2: FILE BASED ---
    base = "trajectories"
    if not os.path.exists(base): return jsonify(points=[])
    csvs = [os.path.join(base, f) for f in os.listdir(base) if f.endswith(".csv")]
    if not csvs: return jsonify(points=[])

    target_file = None
    req_file = request.args.get('filename')

    if req_file:
        candidate = os.path.join(base, req_file)
        if os.path.exists(candidate) and candidate.endswith(".csv"):
            target_file = candidate

    if not target_file:
        target_file = max(csvs, key=os.path.getmtime)

    points = []
    try:
        with open(target_file, newline='') as f:
            reader = csv.DictReader(f)
            keys = reader.fieldnames or []

            lat_k = next((k for k in keys if "lat" in k.lower()), None)
            lon_k = next((k for k in keys if "lon" in k.lower() or "lng" in k.lower()), None)
            alt_k = next((k for k in keys if any(x in k.lower() for x in ["elev", "alt", "height", "z"])), None)
            time_k = next((k for k in keys if "time" in k.lower() or "date" in k.lower()), None)

            if not lat_k or not lon_k:
                 return jsonify(points=[], error="Missing lat/lon columns")

            for row in reader:
                try:
                    p_lat = float(row[lat_k])
                    p_lon = float(row[lon_k])
                    p_alt = 0.0
                    if alt_k and row[alt_k].strip():
                         try: p_alt = float(row[alt_k])
                         except: pass

                    pt = {"lat": p_lat, "lon": p_lon, "alt": p_alt}
                    if time_k and row[time_k]: pt["time"] = row[time_k]
                    points.append(pt)
                except (ValueError, TypeError):
                    continue

        if len(points) > 3000:
            step = len(points) // 3000
            points = points[::step]

        return jsonify(points=points, source=os.path.basename(target_file))
    except Exception as e:
        return jsonify(points=[], error=str(e))

@app.route("/trajectories/<path:filename>")
def serve_trajectory(filename):
    resp = send_from_directory("trajectories", filename)
    if filename.endswith(".kml"):
        resp.headers["Content-Type"] = "application/vnd.google-earth.kml+xml"
    return resp

@app.route("/clear_trajectories", methods=["POST"])
def clear_trajectories_route():
    clear_directory_contents("trajectories")
    return jsonify(ok=True)

if __name__ == "__main__":
    if not os.path.exists("templates"):
        os.makedirs("templates")
    # Always reset status to idle on startup — prevents zombie "running" state
    # from a previous crash persisting into the new server session.
    # Also clean up any stale lock or tmp files left by a crash.
    for _stale in [STATUS_LOCK_PATH, STATUS_PATH + ".tmp"]:
        try: os.remove(_stale)
        except OSError: pass
    _write_status_file(dict(DEFAULT_STATUS))
    SIM_STATUS.update(dict(DEFAULT_STATUS))
    app.run(debug=True, host="0.0.0.0", port=5000)
