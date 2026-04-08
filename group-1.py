# %%
from pathlib import Path

import pandas as pd

# Download the data folder at this link: https://zenodo.org/records/999150/files/RE-Europe_dataset_package_v1-2.zip?download=1
# Place the folder at the root level of this repo, i.e., after downloading the data
# you should have the following folder structure:
# .
# ├── group-1.py
# └── RE-Europe_dataset_package

ROOT = Path("RE-Europe_dataset_package")

# ---------- load files ----------
nodes = pd.read_csv(ROOT / "Static_data" / "network_nodes.csv")
wind = pd.read_csv(ROOT / "Nodal_TS" / "wind_signal_ECMWF.csv", index_col=0)
solar = pd.read_csv(ROOT / "Nodal_TS" / "solar_signal_ECMWF.csv", index_col=0)
# %%
# ---------- standardize time ----------
wind.index = pd.to_datetime(wind.index)
solar.index = pd.to_datetime(solar.index)

# ---------- inspect node columns ----------
print(nodes.columns.tolist())
# Adjust these names if needed after inspecting:
NODE_COL = "ID"  # or "node", "bus_id", ...
LAT_COL = "latitude"  # or "latitude"
LON_COL = "longitude"  # or "longitude"
COUNTRY_COL = "country"  # or "country_code"
# %%
# ---------- make node ids strings for safe merge ----------
nodes[NODE_COL] = nodes[NODE_COL].astype(str)
wind.columns = wind.columns.astype(str)
solar.columns = solar.columns.astype(str)

# %%
# ---------- wide -> long ----------
wind_long = (
    wind.rename_axis("time")
    .reset_index()
    .melt(id_vars="time", var_name="node", value_name="wind_cf")
)

solar_long = (
    solar.rename_axis("time")
    .reset_index()
    .melt(id_vars="time", var_name="node", value_name="solar_cf")
)
# %%
# ---------- merge ----------
nodes2 = nodes.rename(
    columns={
        "ID": "node",
        "latitude": "lat",
        "longitude": "lon",
        "country": "country",
    }
)[["node", "lat", "lon", "country"]]

nodes2["node"] = nodes2["node"].astype(str)
wind_long["node"] = wind_long["node"].astype(str)
solar_long["node"] = solar_long["node"].astype(str)

group1 = (
    wind_long.merge(solar_long, on=["time", "node"], how="inner")
    .merge(nodes2, on="node", how="left")
    .sort_values(["time", "node"])
    .reset_index(drop=True)
)
# %%
# ---------- add useful time features ----------
group1["year"] = group1["time"].dt.year
group1["month"] = group1["time"].dt.month
group1["hour"] = group1["time"].dt.hour
group1["season"] = pd.Categorical(
    pd.Series(group1["month"]).map(
        {
            12: "winter",
            1: "winter",
            2: "winter",
            3: "spring",
            4: "spring",
            5: "spring",
            6: "summer",
            7: "summer",
            8: "summer",
            9: "autumn",
            10: "autumn",
            11: "autumn",
        }
    ),
    categories=["winter", "spring", "summer", "autumn"],
    ordered=True,
)
# %%
# ---------- EU hourly aggregates ----------
eu_hourly = (
    group1.groupby("time", as_index=False)[["wind_cf", "solar_cf"]]
    .mean()
    .rename(columns={"wind_cf": "wind_cf_eu", "solar_cf": "solar_cf_eu"})
)

eu_hourly["year"] = eu_hourly["time"].dt.year
eu_hourly["month"] = eu_hourly["time"].dt.month
eu_hourly["hour"] = eu_hourly["time"].dt.hour
eu_hourly["season"] = pd.Categorical(
    pd.Series(eu_hourly["month"]).map(
        {
            12: "winter",
            1: "winter",
            2: "winter",
            3: "spring",
            4: "spring",
            5: "spring",
            6: "summer",
            7: "summer",
            8: "summer",
            9: "autumn",
            10: "autumn",
            11: "autumn",
        }
    ),
    categories=["winter", "spring", "summer", "autumn"],
    ordered=True,
)

# ---------- save ----------
out = ROOT / "group1_prepared"
out.mkdir(exist_ok=True)

group1.to_parquet(out / "group1_hourly.parquet", index=False)
group1.to_csv(out / "group1_hourly.csv", index=False)

eu_hourly.to_csv(out / "group1_eu_hourly.csv", index=False)

print(group1.head())
print(eu_hourly.head())
print(f"Saved to {out}")
# %%
# %%
