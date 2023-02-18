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
  series_code::Union{Array{Int64}, UnitRange{Int64}, Int64}=1,
  level = 4)

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
  http_download = Downloads.download(url_CES_datatype);
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
    rename!(df_CES, :year => :date_y)
  elseif frequency==:monthly
    @rsubset!(df_CES, :period != "M13")
    @rtransform!(df_CES, :date = Date(:year, parse(Int, :period[2:3])))
  end 

  if (seasonal_adjustment==true) & (frequency==:monthly)
    @rsubset!(df_CES, string(:seasonal_code) == "S")
  else 
    @rsubset!(df_CES, string(:seasonal_code) == "U")
  end

  @rsubset!(df_CES, :data_type_code in series_code)

  select!(df_CES, r"date", :naics_code, :value, 
      :industry_code, :industry_name, :data_type_text, :series_id)
  rename!(df_CES, :naics_code => Symbol(string(industry)*"_"*string(level)) )

  return(df_CES)

end


function build_CES(;
  industry::Symbol=:naics,
  frequency::Symbol=:monthly,
  seasonal_adjustment::Bool=true,
  series_code::Union{Array{Int64}, UnitRange{Int64}, Int64}=1,
  level = 4)

  df_CES = download_CES();
  df_CES = build_CES(df_CES;
    industry,
    frequency, 
    seasonal_adjustment, 
    series_code,
    level
    )

  return(df_CES)

end 

# build_CES(frequency=:annual, seasonal_adjustment=true, series_code=1, level=4)
# ---------------------------------------------------------



# ---------------------------------------------------------
"""
    download_CES(year_target::Int)

Download and import the CBP for a specific year
"""
function download_CES_sic()

# list files with average weekly earnings

  df_CES = DataFrame()
  url_list = ["ee.data.27.TotsAWCurr", "ee.data.28.ManufactureAWCurr",
    "ee.data.29.ServiceProdTPUAWCurr", "ee.data.30.TradeAWCurr", 
    "ee.data.31.FireAWCurr", "ee.data.32.ServicesAWCurr"]

  for url_iter in url_list
    url_tmp = "https://download.bls.gov/pub/time.series/ee/" * url_iter
    http_request = Downloads.request(url_tmp);
    if http_request.status == 404
        @warn  "Error 404 (cannot download file) for CES file: " * url_iter
        return DataFrame()
    end
    http_download = Downloads.download(url_tmp);
    df_CES_tmp = CSV.File(http_download) |> DataFrame;
    rename!(strip, df_CES_tmp)
    df_CES = vcat(df_CES_tmp, df_CES)
  end

  return df_CES

end
# ---------------------------------------------------------


# ---------------------------------------------------------
function build_CES_sic(df_CES;
  frequency::Symbol=:monthly,
  seasonal_adjustment::Bool=false,
  series_code::Union{Array{Int64}, UnitRange{Int64}, Int64}=4
  )

  df_CES_tmp = @rtransform(df_CES, 
    :survey         = :series_id[1:2],
    :seasonal_code  = :series_id[3],
    :industry_code  = parse(Int, :series_id[4:9]),
    :data_type_code = parse(Int, :series_id[10:11]))
  
  url_CES_industry = "https://download.bls.gov/pub/time.series/ee/ee.industry"
  http_download = Downloads.download(url_CES_industry);
  df_CES_industry = CSV.File(http_download) |> DataFrame;
  rename!(strip, df_CES_industry)

  url_CES_datatype = "https://download.bls.gov/pub/time.series/ee/ee.datatype"
  http_download = Downloads.download(url_CES_datatype);
  df_CES_datatype = CSV.File(http_download) |> DataFrame;
  rename!(strip, df_CES_datatype)

  # if we only keep three digits codes:
  @rsubset!(df_CES_industry, :industry_code >= 1E5 ) # 6 digits and more
  @rtransform!(df_CES_industry, :industry_code = string(:industry_code))
  @rsubset!(df_CES_industry, :industry_code[6:6]  == "0" ) # remove 4 digits sic
  @rsubset!(df_CES_industry, :SIC_code != "N/A")
  @rsubset!(df_CES_industry, :industry_code[5:5]  != "0" ) # remove 2 digits sic
  @rtransform!(df_CES_industry, :len_sic = length(:SIC_code)) 

  # some of the double that we deal with (simple here)  
  df_CES_industry_tmp = @select(@subset(df_CES_industry, :len_sic .> 3), :industry_code, :SIC_code, :industry_name)
  @eachrow! df_CES_industry_tmp begin
    @newcol :SIC1::Vector{String}
    @newcol :SIC2::Vector{String}
    z_sub   = lstrip.(split(:SIC_code, ","))
    str_tmp = map(x->z_sub[1][1:(length(z_sub[1])-1)].*last(x), z_sub)
    :SIC1 = str_tmp[1]
    :SIC2 = str_tmp[2]
  end
  df_CES_industry_tmp = stack(@select(df_CES_industry_tmp, $(Not(:SIC_code)) ), [:SIC1, :SIC2])
  sort!(df_CES_industry_tmp, :industry_code)
  @select!(df_CES_industry_tmp, :industry_code, :SIC_code=:value, :industry_name)
  df_CES_industry = vcat(
    @select(@subset(df_CES_industry, :len_sic .<= 3), :industry_code, :SIC_code, :industry_name),
     df_CES_industry_tmp)
  @rtransform!(df_CES_industry, :industry_code = parse(Int, :industry_code) );

  df_CES_sic3 = innerjoin(df_CES_tmp, df_CES_industry, on = :industry_code);
  @select!(df_CES_sic3, :year, :value, :data_type_code, :SIC_code, :period, :seasonal_code)
  df_CES_sic3 = leftjoin(df_CES_sic3, df_CES_datatype, on=:data_type_code);

  if frequency==:annual
    @rsubset!(df_CES_sic3, :period == "M13")
    rename!(df_CES_sic3, :year => :date_y)
  elseif frequency==:monthly
    @rsubset!(df_CES_sic3, :period != "M13")
    @rtransform!(df_CES_sic3, :date_m = Date(:year, parse(Int, :period[2:3])))
  end 
  if seasonal_adjustment==true
    @warn "Not available on this dataset"
  else 
    @rsubset!(df_CES_sic3, :seasonal_code == 'U')
  end
  @rsubset!(df_CES_sic3, :data_type_code in series_code)
  select!(df_CES_sic3, r"date", :SIC_code=>:sic_3, :value, :data_type_text)

  return(df_CES_sic3)

end

function build_CES_sic(;
  frequency::Symbol=:monthly,
  seasonal_adjustment::Bool=false,
  series_code::Union{Array{Int64}, UnitRange{Int64}, Int64}=4
  )

  df_CES_raw = download_CES_sic();
  df_CES = build_CES_sic(df_CES_raw;
    frequency, 
    seasonal_adjustment, 
    series_code
    )

  return(df_CES)

end 

# build_CES_sic(frequency=:annual, seasonal_adjustment=false, series_code=4)
# ---------------------------------------------------------

