# FirmDynamics

[![Build Status](https://github.com/eloualiche/FirmDynamics.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/eloualiche/FirmDynamics.jl/actions/workflows/CI.yml?query=branch%3Amain)


## Installation

Package is not registered. To add directly from github:

```julia
import Pkg
Pkg.add(url="https://github.com/eloualiche/FirmDynamics.jl")
```

## County Business Pattern

See `ImportCBP.jl` which export two functions. 
Using a collection of dates, download for a given level of aggregation for industry types (specify `naics` or `sic` depending on which years you want to download).

```julia
using FirmDynamics
df_CBP_naics = FirmDynamics.build_CBP(2018:2020, 
  aggregation=:county, industry=:naics);
```

A function aggregates directly the CBP into employment by cells (geographical unit x industry unit).

```julia
df_CBP_emp_naics4 = FirmDynamics.build_emp_CBP(2018:2020, 
  aggregation=:county, industry=:naics, level=4);

# similar result can be achieved using data downloaded above
df_CBP_emp_naics4 = FirmDynamics.build_emp_CBP(df_CBP_naics, 
  aggregation=:county, industry=:naics, level=4)
```

## Current Employment Statistics

See the flat files 
  - Current files (naics aggregation) [here](https://download.bls.gov/pub/time.series/ce.txt)
  - Legacy files (sic aggregation) [here](https://download.bls.gov/pub/time.series/ee/ee.txt)


Read and select the relevant series in one movement:

```julia
df_CES_earnings = FirmDynamics.build_CES(
  industry=:naics, frequency=:monthly,
  seasonal_adjustment=true, 
  series_code=[1, 11, 15, 30, 33], 
  level = 4)
```

