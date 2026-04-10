library(tidyverse)
library(arrow)

# Load dataset
dataset <- read_parquet("dataset/renewables-dataset.parquet")

# Compute residual for given penetration
a_s <- 0.30 # solar matches (a_s x 100)% of the average yearly demand across EU
a_w <- 0.05 # wind matches (a_w x 100)% of the average yearly demand across EU

dataset_residual <- dataset %>%
  mutate(
    solar_scaled_MWh = a_s * solar_MWh,
    wind_scaled_MWh = a_w * wind_MWh,
    supply_scaled_MWh = solar_scaled_MWh + wind_scaled_MWh, 
    residual_MWh = demand_MWh - supply_scaled_MWh) %>% 
  select(Time, ID, solar_scaled_MWh, wind_scaled_MWh, demand_MWh, supply_scaled_MWh, residual_MWh)


# Plot metrics over a few days at a specific station 
subset_data <- dataset_residual %>% 
  filter(ID == 1) %>% 
  select(-ID) %>% 
  filter(
    year(Time) == 2013,
    month(Time) == 8,
    day(Time) %in% c(7, 8, 9, 10)
  ) %>% 
  pivot_longer(-all_of("Time"), names_to = "source", values_to = "MWh")

ggplot(subset_data) +
  geom_line(aes(x = Time, y = MWh, col = source))

