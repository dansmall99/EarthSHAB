from flask import (
    Flask,
    request,
    render_template_string,
    jsonify,
    send_from_directory,
    url_for,
)
import threading
import os
import csv
import glob
from datetime import datetime
import io
from contextlib import redirect_stdout, redirect_stderr
import importlib
import time
import subprocess


import netCDF4
import config_earth
import main as earth_main

app = Flask(__name__)
app.secret_key = "earthshab-secret"

# ---------------------------------------------------------------------
# Global simulation status (for /run + /status)
# ---------------------------------------------------------------------
SIM_STATUS = {
    "progress": 0,
    "message": "Idle",
    "running": False,
    "error": None,
    "log": "",
}


def set_status(progress=None, message=None, running=None, error=None, log=None):
    if progress is not None:
        SIM_STATUS["progress"] = int(progress)
    if message is not None:
        SIM_STATUS["message"] = str(message)
    if running is not None:
        SIM_STATUS["running"] = bool(running)
    if error is not None:
        SIM_STATUS["error"] = str(error) if error else None
    if log is not None:
        SIM_STATUS["log"] = str(log)


# ---------------------------------------------------------------------
# Forecast helpers (no autoNETCDF on startup)
# ---------------------------------------------------------------------
REQUIRED_GFS_VARS = ["ugrdprs", "vgrdprs", "hgtprs"]


def _forecast_time_to_str(ft):
    if isinstance(ft, datetime):
        return ft.strftime("%Y-%m-%d %H:%M:%S")
    return str(ft)


def expected_gfs_nc_path(forecast_start_time):
    """
    forecasts/gfs_0p25_YYYYMMDD_HH.nc
    """
    fs = _forecast_time_to_str(forecast_start_time)
    if len(fs) < 13:
        return ""
    year = fs[0:4]
    month = fs[5:7]
    day = fs[8:10]
    hour = fs[11:13]
    return f"forecasts/gfs_0p25_{year}{month}{day}_{hour}.nc"


def is_valid_gfs_file(nc_path, start_time=None):
    """
    Check that the given NetCDF file:
      - exists
      - opens
      - has the required GFS variables

    If start_time is provided, we *also* try to check that it lies within
    the file's time coverage. If we can't do that robustly (e.g. timezone
    issues), we log a warning but do NOT reject the file on that basis.
    """
    if not nc_path or not os.path.exists(nc_path):
        print(f"[GFS VALIDATION] File does not exist: {nc_path}")
        return False

    try:
        ds = netCDF4.Dataset(nc_path, "r")
    except Exception as e:
        print(f"[GFS VALIDATION] Could not open {nc_path}: {e}")
        return False

    try:
        # Check required variables
        for v in REQUIRED_GFS_VARS:
            if v not in ds.variables:
                print(f"[GFS VALIDATION] Missing variable '{v}' in {nc_path}")
                return False

        if start_time is not None:
            try:
                time_var = ds.variables["time"]
                times = netCDF4.num2date(
                    time_var[:],
                    time_var.units,
                    getattr(time_var, "calendar", "standard"),
                )
                if len(times) == 0:
                    print(f"[GFS VALIDATION] No time entries in {nc_path}")
                    return False

                t0 = times[0]
                t1 = times[-1]

                def _to_naive(dt):
                    try:
                        return dt.replace(tzinfo=None)
                    except Exception:
                        return dt

                t0 = _to_naive(t0)
                t1 = _to_naive(t1)
                st = _to_naive(start_time)

                if not (t0 <= st <= t1):
                    print(
                        f"[GFS VALIDATION] start_time {st} outside forecast coverage "
                        f"{t0} – {t1} in {nc_path}"
                    )
                    return False

                print(
                    f"[GFS VALIDATION] start_time {st} is within forecast coverage "
                    f"{t0} – {t1} in {nc_path}"
                )

            except Exception as e:
                # If anything goes wrong with the time-axis check, don't hard-fail:
                # treat the file as structurally valid and let the simulation code
                # perform any stricter checks.
                print(
                    f"[GFS VALIDATION] Could not validate time axis in {nc_path} "
                    f"(treating as valid w.r.t time): {e}"
                )

        print(f"[GFS VALIDATION] {nc_path} OK (structure & required variables).")
        return True

    finally:
        try:
            ds.close()
        except Exception:
            pass



def require_existing_forecast():
    """
    For simulation runs: require that there is an existing, valid GFS nc_file.

    We only ever look at the file configured in config_earth.netcdf_gfs["nc_file"].
    We do NOT search for "newer" forecasts or re-download anything here.

    We also check that the simulation start_time is within this file's coverage
    (if we can determine that from the time axis).
    """
    nc_path = config_earth.netcdf_gfs.get("nc_file", "")
    if not nc_path:
        raise RuntimeError(
            "No GFS NetCDF file is configured "
            "(config_earth.netcdf_gfs['nc_file'] is empty). "
            "Click 'Download Current Forecast' to create one."
        )

    if not os.path.isfile(nc_path):
        raise RuntimeError(
            f"GFS NetCDF file does not exist: {nc_path}. "
            "Click 'Download Current Forecast' to recreate it."
        )

    # Check structure + required vars + (if possible) simulation time coverage
    sim_start = config_earth.simulation["start_time"]
    ok = is_valid_gfs_file(nc_path, start_time=sim_start)

    if not ok:
        raise RuntimeError(
            "The existing GFS NetCDF file does not appear to be valid for the "
            f"requested simulation start time ({sim_start}). "
            "Make sure your simulation start time lies within the forecast's "
            "time axis, or download a different forecast if needed."
        )

    return nc_path


# ---------------------------------------------------------------------
# Trajectories + landing point
# ---------------------------------------------------------------------
def build_trajectory_groups():
    base_dir = "trajectories"
    groups = {}
    if not os.path.isdir(base_dir):
        return []

    for fname in sorted(os.listdir(base_dir)):
        path = os.path.join(base_dir, fname)
        if not os.path.isfile(path):
            continue
        root, ext = os.path.splitext(fname)
        ext = ext.lower()
        if ext not in [".html", ".pdf", ".kml", ".csv"]:
            continue
        groups.setdefault(root, []).append({"ext": ext[1:], "filename": fname})

    result = []
    for base_name in sorted(groups.keys()):
        files = sorted(groups[base_name], key=lambda x: x["ext"])
        result.append({"name": base_name, "files": files})
    return result


def get_last_landing_from_latest_csv():
    base_dir = "trajectories"
    if not os.path.isdir(base_dir):
        return None

    latest_csv = None
    latest_mtime = -1.0
    for fname in os.listdir(base_dir):
        if not fname.lower().endswith(".csv"):
            continue
        path = os.path.join(base_dir, fname)
        if not os.path.isfile(path):
            continue
        mtime = os.path.getmtime(path)
        if mtime > latest_mtime:
            latest_mtime = mtime
            latest_csv = path

    if latest_csv is None:
        return None

    try:
        with open(latest_csv, newline="") as f:
            reader = csv.DictReader(f)
            last_row = None
            for row in reader:
                last_row = row
        if not last_row:
            return None

        lat_key = None
        lon_key = None
        for key in last_row.keys():
            lk = key.lower()
            if "lat" in lk and lat_key is None:
                lat_key = key
            if ("lon" in lk or "lng" in lk) and lon_key is None:
                lon_key = key

        if lat_key is None or lon_key is None:
            return None

        return (last_row[lat_key], last_row[lon_key])
    except Exception as e:
        print("[LANDING] Could not parse CSV:", e)
        return None


# ---------------------------------------------------------------------
# Template
# ---------------------------------------------------------------------
PAGE_TEMPLATE = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>EarthSHAB Simulation</title>
  <style>
    body { font-family: sans-serif; margin: 1rem 2rem; }
    fieldset { margin-bottom: 1rem; padding: 0.8rem; }
    legend { font-weight: bold; }
    label { display: block; margin-top: 0.3rem; font-weight: bold; }
    input, select { width: 100%; max-width: 320px; }
    .row { display: flex; flex-wrap: wrap; gap: 1.5rem; }
    .col { flex: 1 1 320px; min-width: 280px; }

    #progress-container { margin-top: 1rem; display: none; max-width: 640px; }
    #progress-bar {
      width: 100%;
      height: 20px;
      background-color: #eee;
      border-radius: 4px;
      overflow: hidden;
      margin-bottom: 0.3rem;
    }
    #progress-bar-inner {
      width: 0%;
      height: 100%;
      background-color: #4caf50;
      transition: width 0.3s ease;
    }
    #log-output {
      width: 100%;
      height: 200px;
      font-family: monospace;
      font-size: 0.85em;
      white-space: pre;
    }
  </style>
</head>
<body>
  <h1>EarthSHAB Simulation</h1>

  <form id="config-form" method="post">
    <div class="row">
      <div class="col">
        <fieldset>
          <legend>Forecast Settings</legend>
          <label for="forecast_type">Forecast Type</label>
          <select name="forecast_type" id="forecast_type">
            <option value="GFS" {% if forecast_type == 'GFS' %}selected{% endif %}>GFS</option>
            <option value="ERA5" {% if forecast_type == 'ERA5' %}selected{% endif %}>ERA5</option>
          </select>

          <label for="forecast_start_time">Forecast Start Time (UTC)</label>
          <input type="datetime-local" id="forecast_start_time" name="forecast_start_time"
                 value="{{ forecast_start_time_local }}" />

          <label for="gfsrate">GFS Rate (s)</label>
          <input type="number" id="gfsrate" name="gfsrate" step="1" value="{{ gfsrate }}" />

          <label for="netcdf_gfs_nc_file">GFS NetCDF file</label>
          <input type="text" id="netcdf_gfs_nc_file" name="netcdf_gfs_nc_file"
                 value="{{ netcdf_gfs_nc_file }}" />

          <label for="netcdf_era5_filename">ERA5 filename</label>
          <input type="text" id="netcdf_era5_filename" name="netcdf_era5_filename"
                 value="{{ netcdf_era5_filename }}" />
          <hr style="margin-top:0.8rem; margin-bottom:0.8rem;">

          {% if coverage_start and coverage_end %}
          <p>
            <strong>Forecast coverage:</strong><br>
            {{ coverage_start }} &nbsp;–&nbsp; {{ coverage_end }}
          </p>

          <label for="coverage_time_select">Valid forecast times</label>
          <select id="coverage_time_select" style="max-width: 100%;">
            {% for t in coverage_times %}
              <option value="{{ t }}">{{ t.replace("T", " ") }}</option>
            {% endfor %}
          </select>

          <button type="button" id="use-coverage-time-btn" style="margin-top:0.4rem;">
            Use selected time as simulation start
          </button>
          {% else %}
          <p><em>No coverage info yet. Download a forecast to see valid times.</em></p>
          {% endif %}

          <p style="margin-top:0.5rem;">
            <button type="button" id="run-simulation-btn">Run Simulation</button>
          </p>
          <p>
            <button type="submit"
                    id="download-forecast-btn"
                    formaction="{{ url_for('download_forecast') }}"
                    formmethod="post">
              Download Current Forecast
            </button>
          </p>
        </fieldset>

        <fieldset>
          <legend>Simulation Settings</legend>

          <label for="start_time">Simulation Start Time (UTC)</label>
          <input type="datetime-local" id="start_time" name="start_time"
                 value="{{ start_time_local }}" />

          <label for="sim_time">Simulation Duration (hours)</label>
          <input type="number" id="sim_time" name="sim_time" step="1" value="{{ sim_time }}" />

          <label for="vent">Vent Mass Flow (kg/s)</label>
          <input type="number" id="vent" name="vent" step="0.001" value="{{ vent }}" />

          <label for="alt_sp">Altitude Setpoint (m)</label>
          <input type="number" id="alt_sp" name="alt_sp" step="1" value="{{ alt_sp }}" />

          <label for="v_sp">Velocity Setpoint (m/s)</label>
          <input type="number" id="v_sp" name="v_sp" step="0.1" value="{{ v_sp }}" />

          <label for="dt">Time Step dt (s)</label>
          <input type="number" id="dt" name="dt" step="0.1" value="{{ dt }}" />

          <label>Start Coordinate</label>
          <input type="number" step="0.000001" name="lat" value="{{ lat }}" placeholder="Latitude (deg)" />
          <input type="number" step="0.000001" name="lon" value="{{ lon }}" placeholder="Longitude (deg)" />
          <input type="number" step="1" name="start_alt" value="{{ start_alt }}" placeholder="Altitude (m)" />

          <label for="min_alt">Min Altitude (m)</label>
          <input type="number" id="min_alt" name="min_alt" step="1" value="{{ min_alt }}" />

          <label for="float_alt">Float Altitude (m)</label>
          <input type="number" id="float_alt" name="float_alt" step="1" value="{{ float_alt }}" />

          <label for="balloon_trajectory">Balloon Trajectory CSV (optional)</label>
          <input type="text" id="balloon_trajectory" name="balloon_trajectory"
                 value="{{ balloon_trajectory }}" />
        </fieldset>
      </div>

      <div class="col">
        <fieldset>
          <legend>Balloon Properties</legend>
          <label for="d">Diameter d (m)</label>
          <input type="number" id="d" name="d" step="0.01" value="{{ d }}" />

          <label for="mp">Payload Mass mp (kg)</label>
          <input type="number" id="mp" name="mp" step="0.01" value="{{ mp }}" />

          <label for="mEnv">Envelope Mass mEnv (kg)</label>
          <input type="number" id="mEnv" name="mEnv" step="0.01" value="{{ mEnv }}" />

          <label for="Upsilon">Ascent Resistance Upsilon</label>
          <input type="number" id="Upsilon" name="Upsilon" step="0.1" value="{{ Upsilon }}" />

          <label for="absEnv">Absorptivity absEnv</label>
          <input type="number" id="absEnv" name="absEnv" step="0.01" value="{{ absEnv }}" />

          <label for="emissEnv">Emissivity emissEnv</label>
          <input type="number" id="emissEnv" name="emissEnv" step="0.01" value="{{ emissEnv }}" />
        </fieldset>

        <fieldset>
          <legend>Email & Run</legend>

          <label for="run_name">Run Name (output prefix)</label>
          <input type="text" id="run_name" name="run_name" value="{{ run_name }}" />

          <label for="email_to">Results Email</label>
          <input type="email" id="email_to" name="email_to" value="{{ email_to }}" />
        </fieldset>

        <fieldset id="progress-container">
          <legend>Status</legend>
          <div id="progress-bar">
            <div id="progress-bar-inner"></div>
          </div>
          <div id="progress-text">Idle</div>
          <label for="log-output">Log</label>
          <textarea id="log-output" readonly></textarea>
        </fieldset>

        {% if trajectory_groups %}
        <fieldset>
          <legend>Existing Trajectories</legend>
          <ul>
          {% for group in trajectory_groups %}
            <li>
              <details>
                <summary>{{ group.name }}</summary>
                <ul>
                {% for f in group.files %}
                  <li>
                    {{ f.ext|upper }}:
                    <a href="{{ url_for('serve_trajectory', filename=f.filename) }}" target="_blank">
                      {{ f.filename }}
                    </a>
                  </li>
                {% endfor %}
                </ul>
              </details>
            </li>
          {% endfor %}
          </ul>
          {% if landing_lat and landing_lon %}
          <p><strong>Last landing location:</strong> {{ landing_lat }}, {{ landing_lon }}</p>
          {% endif %}
        </fieldset>
        {% endif %}
      </div>
    </div>
  </form>

  <script>
  document.addEventListener('DOMContentLoaded', function () {
    const form = document.getElementById('config-form');
    const runBtn = document.getElementById('run-simulation-btn');
    const downloadBtn = document.getElementById('download-forecast-btn');
    const progressContainer = document.getElementById('progress-container');
    const barInner = document.getElementById('progress-bar-inner');
    const progressText = document.getElementById('progress-text');
    const logOutput = document.getElementById('log-output');
    const coverageSelect = document.getElementById('coverage_time_select');
    const useCoverageBtn = document.getElementById('use-coverage-time-btn');
    const startTimeInput = document.getElementById('start_time');

    let pollTimer = null;

    function updateProgress() {
      fetch("{{ url_for('sim_status') }}")
        .then(resp => resp.json())
        .then(data => {
          barInner.style.width = data.progress + "%";
          progressText.textContent = data.message + (data.error ? (" (Error: " + data.error + ")") : "");
          if (logOutput && typeof data.log === "string") {
            logOutput.value = data.log;
            logOutput.scrollTop = logOutput.scrollHeight;
          }
          if (!data.running) {
            if (pollTimer) {
              clearInterval(pollTimer);
              pollTimer = null;
            }
            if (data.error) {
              alert("Simulation error: " + data.error);
            } else {
              // reload to refresh trajectories / landing
              window.location.reload();
            }
          }
        })
        .catch(err => console.error("Status error:", err));
    }

    if (runBtn && form) {
      runBtn.addEventListener('click', function (evt) {
        evt.preventDefault();
        const formData = new FormData(form);

        progressContainer.style.display = 'block';
        barInner.style.width = '0%';
        progressText.textContent = 'Starting simulation...';
        if (logOutput) logOutput.value = "";

        fetch("{{ url_for('run_simulation') }}", {
          method: "POST",
          body: formData
        })
        .then(resp => resp.json())
        .then(data => {
          if (data.started) {
            pollTimer = setInterval(updateProgress, 1000);
          } else {
            alert("Could not start simulation: " + (data.error || "unknown error"));
          }
        })
        .catch(err => {
          alert("Error starting simulation: " + err);
        });
      });
    }
    if (downloadBtn && form) {
      downloadBtn.addEventListener('click', function (evt) {
        evt.preventDefault();

        const formData = new FormData(form);

        progressContainer.style.display = 'block';
        barInner.style.width = '0%';
        progressText.textContent = 'Starting forecast download...';
        if (logOutput) {
          logOutput.value = "";
        }

        fetch("{{ url_for('download_forecast_async') }}", {
          method: "POST",
          body: formData
        })
        .then(resp => resp.json())
        .then(data => {
          if (data.started) {
            if (pollTimer) {
              clearInterval(pollTimer);
            }
            pollTimer = setInterval(updateProgress, 1000);
          } else {
            alert("Could not start forecast download: " + (data.error || "unknown error"));
          }
        })
        .catch(err => {
          alert("Error starting forecast download: " + err);
        });
      });
    }
    if (coverageSelect && useCoverageBtn && startTimeInput) {
      useCoverageBtn.addEventListener('click', function () {
        const val = coverageSelect.value;
        if (val) {
          // val is already in "YYYY-MM-DDTHH:MM" format,
          // which is exactly what datetime-local inputs expect.
          startTimeInput.value = val;
        }
      });
    }

  });
  </script>
</body>
</html>
"""


# ---------------------------------------------------------------------
# Context builder
# ---------------------------------------------------------------------
def _get_current_context():
    sim = config_earth.simulation
    forecast = config_earth.forecast
    balloon = config_earth.balloon_properties
    netcdf_gfs = config_earth.netcdf_gfs
    netcdf_era5 = config_earth.netcdf_era5

    start_coord = sim["start_coord"]

    start_time = sim["start_time"]
    if isinstance(start_time, datetime):
        start_time_local = start_time.strftime("%Y-%m-%dT%H:%M")
    else:
        start_time_local = ""

    forecast_start_time = forecast["forecast_start_time"]
    if isinstance(forecast_start_time, datetime):
        forecast_start_time_local = forecast_start_time.strftime("%Y-%m-%dT%H:%M")
    else:
        try:
            dt_fs = datetime.fromisoformat(str(forecast_start_time).replace(" ", "T"))
            forecast_start_time_local = dt_fs.strftime("%Y-%m-%dT%H:%M")
        except Exception:
            forecast_start_time_local = ""

    trajectory_groups = build_trajectory_groups()
    landing = get_last_landing_from_latest_csv()
    if landing:
        landing_lat, landing_lon = landing
    else:
        landing_lat, landing_lon = None, None

 # --- Forecast coverage info from current nc_file (if any) ---
    coverage_start = None
    coverage_end = None
    coverage_times = []

    nc_path = netcdf_gfs.get("nc_file", "")
    if nc_path and os.path.exists(nc_path):
        try:
            ds = netCDF4.Dataset(nc_path, "r")
            try:
                time_var = ds.variables["time"]
                times = netCDF4.num2date(
                    time_var[:],
                    time_var.units,
                    getattr(time_var, "calendar", "standard"),
                )
                if len(times) > 0:
                    def _to_naive(dt):
                        try:
                            return dt.replace(tzinfo=None)
                        except Exception:
                            return dt

                    times_naive = [_to_naive(t) for t in times]
                    coverage_start = times_naive[0].strftime("%Y-%m-%d %H:%M")
                    coverage_end = times_naive[-1].strftime("%Y-%m-%d %H:%M")

                    # Build a list of valid start times in datetime-local format
                    for t in times_naive:
                        coverage_times.append(t.strftime("%Y-%m-%dT%H:%M"))
            finally:
                ds.close()
        except Exception as e:
            print(f"[COVERAGE] Could not read coverage from {nc_path}: {e}")


    email_to = getattr(config_earth, "email_to", "youruserid@gmail.com")
    run_name = getattr(config_earth, "run_name", "SHAB9")

    return dict(
        forecast_type=forecast["forecast_type"],
        forecast_start_time_local=forecast_start_time_local,
        gfsrate=forecast["GFSrate"],
        netcdf_gfs_nc_file=netcdf_gfs.get("nc_file", ""),
        netcdf_era5_filename=netcdf_era5.get("filename", ""),

        start_time_local=start_time_local,
        sim_time=sim["sim_time"],
        vent=sim["vent"],
        alt_sp=sim["alt_sp"],
        v_sp=sim["v_sp"],
        dt=sim["dt"],

        lat=start_coord["lat"],
        lon=start_coord["lon"],
        start_alt=start_coord["alt"],
        min_alt=sim["min_alt"],
        float_alt=sim["float"],
        balloon_trajectory=sim.get("balloon_trajectory") or "",

        d=balloon["d"],
        mp=balloon["mp"],
        mEnv=balloon["mEnv"],
        Upsilon=balloon["Upsilon"],
        absEnv=balloon["absEnv"],
        emissEnv=balloon["emissEnv"],

        trajectory_groups=trajectory_groups,
        landing_lat=landing_lat,
        landing_lon=landing_lon,
        email_to=email_to,
        run_name=run_name,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        coverage_times=coverage_times,

    )


# ---------------------------------------------------------------------
# Progress callback factory for main.run_simulation
# ---------------------------------------------------------------------
def make_progress_callback(log_buf):
    def _cb(fraction, message=None):
        try:
            frac = max(0.0, min(1.0, float(fraction)))
        except Exception:
            frac = 0.0
        pct = 20 + int(75 * frac)  # 20–95
        if message is None:
            message = f"Simulation progress: {int(frac*100)}%"
        set_status(progress=pct, message=message, log=log_buf.getvalue())
    return _cb


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    ctx = _get_current_context()
    return render_template_string(PAGE_TEMPLATE, **ctx)


@app.route("/status", methods=["GET"])
def sim_status():
    return jsonify(SIM_STATUS)


def _run_simulation_worker(form_data):
    try:
        set_status(progress=5, message="Updating configuration...", running=True, error=None, log="")

        # --- Forecast type & start time ---
        forecast_type = form_data.get("forecast_type", "GFS")
        config_earth.forecast["forecast_type"] = forecast_type

        fst_str = form_data.get("forecast_start_time", "").strip()
        if fst_str:
            try:
                dt_fs = datetime.fromisoformat(fst_str)
                config_earth.forecast_start_time = dt_fs.strftime("%Y-%m-%d %H:%M:%S")
                config_earth.forecast["forecast_start_time"] = config_earth.forecast_start_time
            except Exception as e:
                print(f"Warning: could not parse forecast_start_time: {e}")

        gfsrate = form_data.get("gfsrate", "").strip()
        if gfsrate:
            try:
                config_earth.forecast["GFSrate"] = int(float(gfsrate))
            except ValueError:
                print("Warning: invalid GFSrate; keeping previous value.")

        nc_gfs_file_form = form_data.get("netcdf_gfs_nc_file", "").strip()
        if nc_gfs_file_form:
            config_earth.netcdf_gfs["nc_file"] = nc_gfs_file_form.replace("\\", "/")

        nc_era5_filename = form_data.get("netcdf_era5_filename", "").strip()
        if nc_era5_filename:
            config_earth.netcdf_era5["filename"] = nc_era5_filename

        # --- Simulation timing & coords ---
        start_time_str = form_data.get("start_time", "").strip()
        if start_time_str:
            try:
                dt_start = datetime.fromisoformat(start_time_str)
                config_earth.simulation["start_time"] = dt_start
                config_earth.start_time = dt_start
                config_earth.simulation["start_coord"]["timestamp"] = dt_start
            except Exception as e:
                print(f"Warning: could not parse start_time: {e}")

        def _update_float(field, dct, key):
            val = form_data.get(field, "").strip()
            if val:
                try:
                    dct[key] = float(val)
                except ValueError:
                    print(f"Warning: invalid {field}; keeping previous value.")

        def _update_int(field, dct, key):
            val = form_data.get(field, "").strip()
            if val:
                try:
                    dct[key] = int(float(val))
                except ValueError:
                    print(f"Warning: invalid {field}; keeping previous value.")

        _update_int("sim_time", config_earth.simulation, "sim_time")
        _update_float("vent", config_earth.simulation, "vent")
        _update_float("alt_sp", config_earth.simulation, "alt_sp")
        _update_float("v_sp", config_earth.simulation, "v_sp")
        _update_float("dt", config_earth.simulation, "dt")

        _update_float("min_alt", config_earth.simulation, "min_alt")
        _update_float("float_alt", config_earth.simulation, "float")

        start_coord = config_earth.simulation["start_coord"]
        _update_float("lat", start_coord, "lat")
        _update_float("lon", start_coord, "lon")
        _update_float("start_alt", start_coord, "alt")

        traj = form_data.get("balloon_trajectory", "").strip()
        config_earth.balloon_trajectory = traj if traj else None
        config_earth.simulation["balloon_trajectory"] = config_earth.balloon_trajectory

        # --- Balloon props ---
        _update_float("d", config_earth.balloon_properties, "d")
        _update_float("mp", config_earth.balloon_properties, "mp")
        _update_float("mEnv", config_earth.balloon_properties, "mEnv")
        _update_float("Upsilon", config_earth.balloon_properties, "Upsilon")
        _update_float("absEnv", config_earth.balloon_properties, "absEnv")
        _update_float("emissEnv", config_earth.balloon_properties, "emissEnv")

        # --- Run name & email ---
        run_name = form_data.get("run_name", "").strip() or "SHAB9"
        config_earth.run_name = run_name
        email_to = form_data.get("email_to", "").strip() or "youruserid@gmail.com"
        config_earth.email_to = email_to

        # --- Require forecast if GFS ---
        if forecast_type == "GFS":
            set_status(progress=15, message="Checking existing forecast file...")
            require_existing_forecast()

        # --- Patch yagmail to force email_to (optional) ---
        #try:
        #    import yagmail as _yagmail
#
#            class SMTPWrapper(_yagmail.SMTP):
#                def send(self, to=None, subject=None, contents=None, attachments=None, **kwargs):
#                    to_addr = getattr(config_earth, "email_to", "youruserid@gmail.com")
#                    return super().send(
#                        to=to_addr,
#                        subject=subject,
#                        contents=contents,
#                        attachments=attachments,
#                        **kwargs,
#                    )
#
#            _yagmail.SMTP = SMTPWrapper
#        except Exception:
#            pass

        # --- Run simulation with captured log ---
        log_buf = io.StringIO()
        progress_cb = make_progress_callback(log_buf)

        set_status(progress=20, message="Running simulation...", log="")
        try:
            with redirect_stdout(log_buf), redirect_stderr(log_buf):
                earth_main.run_simulation(progress_callback=progress_cb)
        except Exception as e:
            set_status(
                progress=100,
                message="Simulation failed",
                error=str(e),
                running=False,
                log=log_buf.getvalue(),
            )
            return

        set_status(
            progress=100,
            message="Simulation complete",
            error=None,
            running=False,
            log=log_buf.getvalue(),
        )

    except Exception as e:
        set_status(
            progress=100,
            message="Unexpected error",
            error=str(e),
            running=False,
        )


@app.route("/run_simulation", methods=["POST"])
def run_simulation():
    if SIM_STATUS.get("running"):
        return jsonify({"started": False, "error": "Simulation already running"}), 400

    try:
        form_data = request.form.to_dict()
        set_status(progress=0, message="Queued...", running=True, error=None, log="")

        t = threading.Thread(target=_run_simulation_worker, args=(form_data,))
        t.daemon = True
        t.start()

        return jsonify({"started": True})
    except Exception as e:
        return jsonify({"started": False, "error": str(e)}), 500


def _download_forecast_worker_async(form_data):
    """
    Background worker used by /download_forecast_async.
    It:
      - updates forecast settings from form
      - deletes old GFS nc_file (if any)
      - runs autoNETCDF.generate_gfs_netcdf() in a sub-thread
      - every 2 seconds logs `ls -al <nc_file>` and file size
      - stops polling when the file size hasn't changed for ~10 seconds
      - validates the resulting file
      - streams everything into SIM_STATUS['log']
    """
    import autoNETCDF

    log_buf = io.StringIO()

    try:
        set_status(
            progress=5,
            message="Preparing forecast download...",
            running=True,
            error=None,
            log="",
        )

        # --- Forecast type / time from form ---
        forecast_type = form_data.get("forecast_type", "GFS")
        config_earth.forecast["forecast_type"] = forecast_type

        if forecast_type != "GFS":
            final_log = "[Download] Only GFS forecast download is implemented.\n"
            set_status(
                progress=100,
                message="Forecast download aborted (non-GFS)",
                running=False,
                error=None,
                log=final_log,
            )
            return

        fst_str = form_data.get("forecast_start_time", "").strip()
        if fst_str:
            try:
                dt_fs = datetime.fromisoformat(fst_str)
                config_earth.forecast_start_time = dt_fs.strftime("%Y-%m-%d %H:%M:%S")
                config_earth.forecast["forecast_start_time"] = config_earth.forecast_start_time
            except Exception as e:
                print(f"Warning: could not parse forecast_start_time: {e}", file=log_buf)

        # Compute expected nc_file path from forecast_start_time
        fst = config_earth.forecast.get("forecast_start_time")
        expected = expected_gfs_nc_path(fst)
        if expected:
            config_earth.netcdf_gfs["nc_file"] = expected

        nc_path = config_earth.netcdf_gfs.get("nc_file", "")
        if not nc_path:
            final_log = log_buf.getvalue() + "\n[Download] No nc_file path computed from forecast_start_time.\n"
            set_status(
                progress=100,
                message="Forecast download failed (no nc_file path)",
                running=False,
                error="No nc_file path computed",
                log=final_log,
            )
            return

        # Remove any existing file at that path
        log_files = glob.glob('./forecasts/*.nc')

        for file_to_delete in log_files:
            try:
                os.remove(file_to_delete)
                print(f"Removed: {file_to_delete}")
            except OSError as e:
                print(f"Error removing {file_to_delete}: {e}")

        set_status(
            progress=15,
            message=f"Starting autoNETCDF for {os.path.basename(nc_path)}...",
            log=log_buf.getvalue(),
        )

        # --- Run autoNETCDF in a sub-thread so we can poll file size ---
        def _run_autonetcdf():
            with redirect_stdout(log_buf), redirect_stderr(log_buf):
                mod = importlib.reload(autoNETCDF)
                if hasattr(mod, "generate_gfs_netcdf"):
                    mod.generate_gfs_netcdf()

        download_thread = threading.Thread(target=_run_autonetcdf, daemon=True)
        download_thread.start()

        # --- Poll file size / ls -al every 2 seconds until size stable 10 seconds ---
        last_size = None
        last_change = time.time()
        stable_required = 10.0  # seconds
        poll_interval = 2.0     # seconds

        while True:
            if os.path.exists(nc_path):
                try:
                    size = os.path.getsize(nc_path)
                except OSError as e:
                    size = None
                    print(f"[Download] os.path.getsize failed: {e}", file=log_buf)

                # If size changed, update last_change time
                if size is not None and size != last_size:
                    last_size = size
                    last_change = time.time()

                # Run `ls -al` for exactly the path the user mentioned
                try:
                    # IMPORTANT: use the actual path, not the misspelled ./forcasts/
                    proc = subprocess.run(
                        ["ls", "-al", nc_path],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    ls_out = proc.stdout.strip() or proc.stderr.strip()
                except Exception as e:
                    ls_out = f"[Download] ls -al failed for {nc_path}: {e}"

                log_buf.write(ls_out + "\n")
                if size is not None:
                    log_buf.write(f"[Download] Current size: {size} bytes\n")
                log_buf.write("\n")

                set_status(
                    progress=40,
                    message="Downloading GFS forecast (monitoring file size)...",
                    log=log_buf.getvalue(),
                )

            # Condition to stop polling:
            #  - download thread finished AND
            #  - file size hasn't changed for at least 10 seconds (if we ever saw it)
            now = time.time()
            thread_alive = download_thread.is_alive()

            if (not thread_alive) and (last_size is not None) and ((now - last_change) >= stable_required):
                break

            # If thread is not alive AND we never saw a size, give it a short grace
            if (not thread_alive) and (last_size is None) and ((now - last_change) >= stable_required):
                break

            time.sleep(poll_interval)

        # Ensure the download thread has actually finished
        download_thread.join(timeout=5.0)

        # --- Validate resulting file ---
        print(f"Validating forecast file: {nc_path}", file=log_buf)
        set_status(
            progress=80,
            message="Validating downloaded forecast file...",
            log=log_buf.getvalue(),
        )

        ok = is_valid_gfs_file(
            nc_path,
            start_time=config_earth.simulation["start_time"],
        )

        final_log = log_buf.getvalue()

        if not ok:
            set_status(
                progress=100,
                message="Forecast download failed (invalid file)",
                running=False,
                error="Downloaded forecast file is invalid for current simulation time",
                log=final_log,
            )
            return

        set_status(
            progress=100,
            message=f"Forecast download complete ({os.path.basename(nc_path)})",
            running=False,
            error=None,
            log=final_log,
        )

    except Exception as e:
        final_log = log_buf.getvalue()
        set_status(
            progress=100,
            message="Forecast download error",
            running=False,
            error=str(e),
            log=final_log,
        )

@app.route("/download_forecast_async", methods=["POST"])
def download_forecast_async():
    """
    Start a forecast download in a background thread.

    The front-end will:
      - POST here with the form data
      - On {"started": true}, show the progress bar and begin polling /status
      - /status will stream autoNETCDF output into the small log window
    """
    if SIM_STATUS.get("running"):
        return jsonify({"started": False, "error": "Another task is already running"}), 400

    try:
        form_data = request.form.to_dict()
        set_status(progress=0, message="Queued forecast download...", running=True, error=None, log="")

        t = threading.Thread(target=_download_forecast_worker_async, args=(form_data,))
        t.daemon = True
        t.start()

        return jsonify({"started": True})
    except Exception as e:
        return jsonify({"started": False, "error": str(e)}), 500


@app.route("/download_forecast", methods=["POST"])
def download_forecast():
    """
    Synchronous forecast download: normal form POST here, shows HTML log page.
    """
    import autoNETCDF
    import importlib

    form_data = request.form.to_dict()
    log_buf = io.StringIO()

    try:
        forecast_type = form_data.get("forecast_type", "GFS")
        config_earth.forecast["forecast_type"] = forecast_type
        if forecast_type != "GFS":
            return f"<h2>Download only supports GFS</h2><p><a href='{url_for('index')}'>Back</a></p>"

        fst_str = form_data.get("forecast_start_time", "").strip()
        if fst_str:
            try:
                dt_fs = datetime.fromisoformat(fst_str)
                config_earth.forecast_start_time = dt_fs.strftime("%Y-%m-%d %H:%M:%S")
                config_earth.forecast["forecast_start_time"] = config_earth.forecast_start_time
            except Exception as e:
                print(f"Warning: could not parse forecast_start_time: {e}", file=log_buf)

        fst = config_earth.forecast.get("forecast_start_time")
        expected = expected_gfs_nc_path(fst)
        if expected:
            config_earth.netcdf_gfs["nc_file"] = expected

        nc_path = config_earth.netcdf_gfs.get("nc_file", "")
        if nc_path and os.path.isfile(nc_path):
            print(f"Removing old forecast file: {nc_path}", file=log_buf)
            os.remove(nc_path)

        print("Running autoNETCDF...", file=log_buf)
        with redirect_stdout(log_buf), redirect_stderr(log_buf):
            autoNETCDF = importlib.reload(autoNETCDF)
            if hasattr(autoNETCDF, "generate_gfs_netcdf"):
                autoNETCDF.generate_gfs_netcdf()

        nc_path = config_earth.netcdf_gfs.get("nc_file", "")
        print(f"Validating forecast file: {nc_path}", file=log_buf)
        ok = is_valid_gfs_file(nc_path, start_time=config_earth.simulation["start_time"])

        final_log = log_buf.getvalue().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        if not ok:
            return f"""
            <h2>Forecast download failed</h2>
            <p>Downloaded forecast file is invalid; see log below.</p>
            <pre>{final_log}</pre>
            <p><a href="{url_for('index')}">Back to main page</a></p>
            """

        return f"""
        <h2>Forecast download complete</h2>
        <p>File: {nc_path}</p>
        <pre>{final_log}</pre>
        <p><a href="{url_for('index')}">Back to main page</a></p>
        """

    except Exception as e:
        final_log = log_buf.getvalue().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return f"""
        <h2>Forecast download error</h2>
        <p>{str(e)}</p>
        <pre>{final_log}</pre>
        <p><a href="{url_for('index')}">Back to main page</a></p>
        """


@app.route("/trajectories/<path:filename>")
def serve_trajectory(filename):
    return send_from_directory("trajectories", filename)


if __name__ == "__main__":
    app.run(debug=True)
