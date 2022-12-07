# ---------------------------------------------------------
# ImportCES.jl

# Download and process file from Current Employment Statistics
# https://download.bls.gov/pub/time.series/ce/ce.txt
# ---------------------------------------------------------


# ---------------------------------------------------------
# List of exported functions
# 
# build_CES
# ---------------------------------------------------------


# ---------------------------------------------------------
"""
    download_CES(year_target::Int)

Download and import the CBP for a specific year
"""
function download_CES()

  url_CES =  "https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries"
  http_request = Downloads.request(url_CES);
  if http_request.status == 404
      @warn  "Error 404 (cannot download file) for CES file"
      return DataFrame()
  end
  http_download = Downloads.download(url_CES);
  df_CES = CSV.File(http_download) |> DataFrame;
  rename!(strip, df_CES)

  return df_CES

end
# ---------------------------------------------------------


# ---------------------------------------------------------
function build_CES(df_CES;
  industry::Symbol=:naics,
  frequency::Symbol=:monthly,
  seasonal_adjustment::Bool=true,
  series_code::Int=1,
  level = 4)

# level = 4
# seasonal_adjustment = true
# frequency = :monthly
# series_code = 1

  @rtransform!(df_CES, 
    :seasonal_code  = :series_id[3],
    :industry_code  = parse(Int, :series_id[4:11]),
    :supersector    = :series_id[4:5],
    :data_type_code = parse(Int, :series_id[12:13]))

  url_CES_industry = "https://download.bls.gov/pub/time.series/ce/ce.industry"
  http_download = Downloads.download(url_CES_industry);
  df_CES_industry = CSV.File(http_download) |> DataFrame;
  rename!(strip, df_CES_industry)

  url_CES_datatype = "https://download.bls.gov/pub/time.series/ce/ce.datatype"
  http_download = Downloads.download(  url_CES_datatype);
  df_CES_datatype = CSV.File(http_download) |> DataFrame;
  rename!(strip,   df_CES_datatype)

  df_CES = leftjoin(df_CES, 
    @select(df_CES_industry, :industry_code, :naics_code, :industry_name),
    on = :industry_code)
  @rtransform!(df_CES, :naics_code = strip(:naics_code))

  df_CES = leftjoin(df_CES, df_CES_datatype, on = :data_type_code)
  @rtransform!(df_CES, :naics_code = strip(:naics_code))

  @select!(df_CES, :year, :period, :naics_code, :value, 
      :seasonal_code, :industry_code, :industry_name, 
      :series_id, :data_type_code, :data_type_text)

  if level in [3,4]
    @rsubset!(df_CES, length(:naics_code) == level )
    @rtransform!(df_CES, :naics_code = parse(Int, :naics_code) )
    elseif level == 5
    @warn "see CES and code"
    @rsubset!(df_CES, length(:naics_code) >= 3 )
    # @rtransform!(df_CES, :naics_code_first = match(r"^([^,])+",  :naics_code).match)
  end

  if frequency==:annual
    @rsubset!(df_CES, :period == "M13")
  elseif frequency==:monthly
    @rsubset!(df_CES, :period != "M13")
    @rtransform!(df_CES, :date = Date(:year, parse(Int, :period[2:3])))
  end 

  if seasonal_adjustment==true
    @rsubset!(df_CES, string(:seasonal_code) == "S")
  else 
    @rsubset!(df_CES, :seasonal_code == "U")
  end

  @rsubset!(df_CES, :data_type_code == series_code)

  @select!(df_CES, :date, :naics_code, :value, 
      :industry_code, :industry_name, :data_type_text, :series_id)

  return(df_CES)

end


function build_CES(;
  industry::Symbol=:naics,
  frequency::Symbol=:monthly,
  seasonal_adjustment::Bool=true,
  series_code::Int=1,
  level = 4)

  df_CES = download_CES(); 
  df_CES = build_CES(df_CES;
    industry, 
    frequency, 
    seasonal_adjustment, 
    series_code,
    level)

  return(df_CES)

end 

@time df_CES = build_CES()
# ---------------------------------------------------------


# ---------------------------------------------------------
