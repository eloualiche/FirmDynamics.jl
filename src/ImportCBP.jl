# ---------------------------------------------------------
# ImportCBP.jl

# Download and process file from CBP
# ---------------------------------------------------------


# ---------------------------------------------------------
# List of exported functions
# 
# build_CBP
# build_emp_CBP
# ---------------------------------------------------------


# ---------------------------------------------------------
"""
    download_CBP(year_target::Int)

Download and import the CBP for a specific year
"""
function download_CBP(year_target::Int; 
  aggregation::Symbol=:county,
  industry::Symbol=:naics)

  if (aggregation == :county)
    suffix = "co"
    group_var = [:fipstate, :fipscty]
  elseif (aggregation == :us)
    suffix = "us"
    group_var = []
  elseif (aggregation == :state)
    suffix = "st"
    group_var = [:fipstate]
   elseif (aggregation == :msa)
    suffix = "msa"
    group_var = [:msa]
    if year_target < 1993
      @warn "No MSA file before 1993"
      return DataFrame(A = Int64[], B = Int64[])
    end
  else 
    @error "Aggregation level not known: " * string(aggregation)
  end

  url_prefix    = "http://www2.census.gov/programs-surveys/cbp/datasets/" * string(year_target) * "/";
  zip_file_name = "cbp" * string(year_target)[3:4] * suffix * ".zip"
  file_name     = "cbp" * string(year_target)[3:4] * suffix * ".txt"

  # Different files have different formats:
  if (aggregation == "us") & (year_target <= 2007)    # national case is not zipped before 2007
    url_CBP =  url_prefix * file_name
    http_response = Downloads.download(url_CBP);
    df_CBP = copy(CSV.File(http_response) |> DataFrame)
  else
    url_CBP =  url_prefix * zip_file_name
    http_request = Downloads.request(url_CBP);
    if http_request.status == 404
      @warn  "Error 404 (cannot download file) for CBP file in year " * string(year_target) * 
        " (" * string(aggregation) * ", " * string(industry) * ")\n" * "... skipping" 
      return DataFrame()
    end
    http_download = Downloads.download(url_CBP);
    z = ZipFile.Reader(http_download) ;
    a_file_in_zip = filter(x -> match(r".*txt", lowercase(x.name)) != nothing, z.files)[1]
    df_CBP = copy(CSV.File(a_file_in_zip) |> DataFrame);
    close(z)
  end 

  # do some cleaning
  rename!(df_CBP, lowercase.(names(df_CBP)))
  df_CBP[!, :year] .= year_target;
  if year_target <= 1997
    if industry == :naics 
      @warn "NAICS files not available before 1997" 
      return DataFrame()
    end
  else
    if industry == :sic 
      @warn "SIC files not available after 1998" 
      return DataFrame()
    end
  end

  col_order = intersect(
    [:year; industry; group_var; :emp; :empflag; :emp_nf],
    Symbol.(names(df_CBP))            )
  select!(df_CBP, col_order, Not(col_order))
  
  return(df_CBP)
end
# ---------------------------------------------------------


# ---------------------------------------------------------
function build_CBP(year_list::Union{Array{Int64}, UnitRange{Int64}}; 
  aggregation::Symbol=:county, 
  industry::Symbol=:naics)

  year_list = collect(year_list)
  if industry==:naics
    if sum(year_list .< 1998)>0 
      @warn "Some NAICS files are not available (pre-1998): " * 
        (map(x -> string(x) * ", ", year_list[ year_list .< 1998 ]) |> prod)[1:end-2]
    end
    year_list = year_list[ year_list .> 1997 ]
  elseif industry==:sic
    if sum(year_list .< 1986)>0 
      @warn "Some SIC files are not available (pre-1986): " * 
        (map(x -> string(x) * ", ", year_list[ year_list .< 1986 ]) |> prod)[1:end-2]
    end
    if sum(year_list .< 1998)>0 
      @warn "Some SIC files are not available (post-1997): " * 
        (map(x -> string(x) * ", ", year_list[ year_list .> 1997 ]) |> prod)[1:end-2]
    end
    year_list = year_list[ (year_list .< 1998) .& (year_list .> 1985) ]
  end

  if isempty(year_list) return DataFrame() end

  @info "Downloading year: " * string(year_list[1])
  df_CBP = download_CBP(year_list[1]; aggregation=aggregation, industry=industry)

  if length(year_list)>1
    for year_iter in year_list[2:end]
      @info "Downloading year: " * string(year_iter)
      df_CBP_tmp = download_CBP(year_iter; aggregation=aggregation, industry=industry)
      if !isempty(df_CBP_tmp)
        df_CBP = vcat(df_CBP, df_CBP_tmp, cols=:union)
      end
    end
  end

  return(df_CBP)

end

# --- version with one year
function build_CBP(year_list::Int64; aggregation::Symbol=:county, industry::Symbol=:naics)
  build_CBP([year_list], aggregation=aggregation, industry=industry)
end
# ---------------------------------------------------------


# ---------------------------------------------------------
# Only one function to aggregate both
function build_CBP_agg(df_CBP::DataFrame;
  aggregation=:county,
  industry=:naics, 
  level = 4,
  verbose=false)

  if (aggregation == :county)
    group_var = [:fipstate, :fipscty]
  elseif (aggregation == :us)
    group_var = []
  elseif (aggregation == :state)
    group_var = [:fipstate]
  elseif (aggregation == :msa)
    group_var = [:msa]
  end

# --- select data based on industry
  if industry == :naics
    df_CBP_tmp = @subset(df_CBP, :year .>= 1998)
  elseif industry == :sic
    df_CBP_tmp = @subset(df_CBP, :year .<= 1997)
  end

# --- correct employment field
if verbose
  @info "Create corrected employment field ..."
end
@transform!(df_CBP_tmp, :emp_corrected = :emp);
if ("emp_flag" in names(df_CBP))
  @eachrow! df_CBP_tmp begin
    if !ismissing(:empflag)
        :emp_corrected = 
         :empflag =="A" ? 10 :
         :empflag =="B" ? 60 :
         :empflag =="C" ? 175 :
         :empflag =="E" ? 375 :
         :empflag =="F" ? 750 :
         :empflag =="G" ? 1750 :
         :empflag =="H" ? 3750 :
         :empflag =="I" ? 7500 :
         :empflag =="J" ? 17500 :
         :empflag =="K" ? 37500 :
         :empflag =="L" ? 75000 :
         :empflag =="M" ? 100000 :
          0;
    end
  end;
end;

# Aggregate and clean up
  if verbose
    @info "Cleaning up industry to the correct level ... " * string(level) * " ..."
  end
  df_CBP_agg = @select(df_CBP_tmp, :year, $industry, $group_var, :emp_corrected, :ap)
  @rtransform!(df_CBP_agg, $industry = replace($industry,  r"(/|-|\\)" => ""))
  @rtransform!(df_CBP_agg, :industry_len   = length($industry))
  
  if (industry==:sic) & (level==3)
    @rsubset!(df_CBP_agg, :industry_len>=3)
    @rsubset!(df_CBP_agg, :industry_len==3 || ( (:industry_len==4) & (:sic[4] == '0')) )
    @rtransform!(df_CBP_agg, :sic_3 = :sic[1:3])        
    sort!(df_CBP_agg, [:year; :sic; group_var])
    @transform!(groupby(df_CBP_agg, [:year; :sic_3; group_var]), :seq_obs=1:size(:sic, 1))
    @rsubset!(df_CBP_agg, :seq_obs == 1)
    select!(df_CBP_agg, Not([:industry_len, :seq_obs, :sic]))
    rename!(df_CBP_agg, :sic_3 => :sic)
  else
    @subset!(df_CBP_agg, :industry_len .== level)
  end

  if verbose
    @info "Aggregation of payroll and employment at industry/date/regional level ..."
  end  
  df_CBP_agg = @combine(groupby(df_CBP_agg, [:year; industry; group_var]),
    :payroll_by_fips = sum(:ap), # in 1000s of dollars
    :emp_by_fips = sum(:emp_corrected) );
  @rtransform!(df_CBP_agg, :wage = :payroll_by_fips / :emp_by_fips);
  sort!(df_CBP_agg, [group_var; :year])
  @transform!(df_CBP_agg, :wage = replace(:wage, NaN=>missing) )
  rename!(df_CBP_agg, :year => :date_y, industry => Symbol(string(industry)*"_"*string(level)) )
  # Compress
  @subset!(df_CBP_agg, :emp_by_fips .> 0 )

  return df_CBP_agg

end

"""
    build_CBP_agg(year_list::Union{Array{Int64}, UnitRange{Int64}}; 
    aggregation=:county, industry=:naics, level = 4, verbose = false)

Download the CBP data and aggregate payments and payrolls by regions and industries for a given level.
"""
function build_CBP_agg(year_list::Union{Array{Int64}, UnitRange{Int64}, Int64};
  aggregation=:county,
  industry=:naics, 
  level = 4,
  verbose=false)

  if industry == :sic
    df_CBP = build_CBP(year_list, aggregation=aggregation, industry=industry);
    df_CBP_agg = build_CBP_agg(df_CBP; aggregation=aggregation, industry=industry, level=level, verbose);

  elseif industry == :naics
    # CREATE THE whole var
    df_CBP = build_CBP(year_list, aggregation=aggregation, industry=industry);
    df_CBP_agg = DataFrame()
    for year_iter in year_list
      @info "aggregating year ... " * string(year_iter)
      df_CBP_tmp = @subset(df_CBP, :year .== year_iter)
      df_CBP_tmp = build_CBP_agg(df_CBP_tmp; aggregation=aggregation, industry=industry, level=level, verbose);
      df_CBP_agg = vcat(df_CBP_agg, df_CBP_tmp, cols=:union)
    end
  end

  return df_CBP_agg

end
# ---------------------------------------------------------


















