library(tidyverse)
library(arrow)


# Static data
node <- read_csv("preparation/RE-Europe_dataset_package/Static_data/network_nodes.csv")
solar_layout <- read_csv("preparation/RE-Europe_dataset_package/Static_data/solar_layouts_ECMWF.csv")
wind_layout <- read_csv("preparation/RE-Europe_dataset_package/Static_data/wind_layouts_ECMWF.csv")


static_data <- node %>% 
  select("ID", "latitude", "longitude", "country") %>% 
  left_join(
    solar_layout %>% 
      rename(ID = node, solar_layout_MW = Proportional) %>% 
      select(ID, solar_layout_MW),
    by = "ID"
  ) %>% 
  left_join(
    wind_layout %>% 
      rename(ID = node, wind_layout_MW = Proportional) %>% 
      select(ID, wind_layout_MW),
    by = "ID"
  ) %>% 
  mutate(
    ID = as.character(ID)
  )


# dynamic data
load_signal <- read_csv("preparation/RE-Europe_dataset_package/Nodal_TS/load_signal.csv") 
solar_signal <- read_csv("preparation/RE-Europe_dataset_package/Nodal_TS/solar_signal_ECMWF.csv")
wind_signal <- read_csv("preparation/RE-Europe_dataset_package/Nodal_TS/wind_signal_ECMWF.csv")

dynamic_signals <- load_signal %>%
  pivot_longer(-all_of("Time"), names_to = "ID", values_to = "demand_MWh") %>% 
  left_join(
    solar_signal %>% 
      pivot_longer(-all_of("Time"), names_to = "ID", values_to = "solar_rel_prod"),
    by = c("Time", "ID")
  ) %>% 
  left_join(
    wind_signal %>% 
      pivot_longer(-all_of("Time"), names_to = "ID", values_to = "wind_rel_prod"),
    by = c("Time", "ID")
  )


# join static with dynamic
dataset <- dynamic_signals %>% 
  left_join(
    static_data,
    by = "ID"
  )


dataset <- dataset %>% 
  mutate(
    solar_MWh = solar_rel_prod * solar_layout_MW,
    wind_MWh = wind_rel_prod * wind_layout_MW,
    supply_MWh = 0.5 * solar_MWh + 0.5 * wind_MWh
  ) %>% 
  select(Time, ID, demand_MWh, supply_MWh, solar_MWh, wind_MWh, everything())



# save dataset
write_parquet(
  dataset,
  "dataset/renewables-dataset.parquet",
  compression = "snappy"   # default
)

