using FirmDynamics
using Test
# ---------------------------------------------------------


# ---------------------------------------------------------
@testset "FirmDynamics.jl" begin

# --- testing the tests
    # @test FirmDynamics.greet_your_package_name() == "Hello YourPackageName!"
    # @test FirmDynamics.greet_your_package_name() != "Hello world!"
    
# --- TESTING ImportCBP.jl
# ----- testing of the SIC files
# note that this has not been tested for sic industries with level different than 3 
   df_CBP_sic = FirmDynamics.build_CBP_agg(1990:1992; 
                  aggregation=:county, industry=:sic, level=3, verbose=true);
   @test size(unique(df_CBP_sic.sic_3), 1) > 50
   @test prod(map(x-> x ∈ names(df_CBP_sic),  
                  ["date_y", "fipstate", "fipscty", "sic_3", 
                   "payroll_by_fips", "emp_by_fips", "wage"]))

   df_CBP_sic = FirmDynamics.build_CBP_agg(1990:1992; 
                  aggregation=:state, industry=:sic, level=3, verbose=true);
   @test size(unique(df_CBP_sic.sic_3), 1) > 50
   @test prod(map(x-> x ∈ names(df_CBP_sic),  
                  ["date_y", "fipstate", "sic_3", 
                   "payroll_by_fips", "emp_by_fips", "wage"]))

# ----- testing the naics files
   # COUNTY
   df_CBP_naics = FirmDynamics.build_CBP_agg(1999:2001; 
                  aggregation=:county, industry=:naics, level=3, verbose=true);
   @test size(unique(df_CBP_naics.naics_3), 1) > 50
   @test prod(map(x-> x ∈ names(df_CBP_naics),  
                  ["date_y", "fipstate", "fipscty", "naics_3", 
                   "payroll_by_fips", "emp_by_fips", "wage"]))
   df_CBP_naics = FirmDynamics.build_CBP_agg(1999:2001; 
                  aggregation=:county, industry=:naics, level=4, verbose=true);
   @test size(unique(df_CBP_naics.naics_4), 1) > 50
   @test prod(map(x-> x ∈ names(df_CBP_naics),  
                  ["date_y", "fipstate", "fipscty", "naics_4", 
                   "payroll_by_fips", "emp_by_fips", "wage"]))
    # MSA
    df_CBP_naics = FirmDynamics.build_CBP_agg(1999:2001; 
                  aggregation=:msa, industry=:naics, level=3, verbose=true);
   @test size(unique(df_CBP_naics.naics_3), 1) > 50
   @test prod(map(x-> x ∈ names(df_CBP_naics),  
                  ["date_y", "msa", "naics_3", 
                   "payroll_by_fips", "emp_by_fips", "wage"]))
   df_CBP_naics = FirmDynamics.build_CBP_agg(1999:2001; 
                  aggregation=:msa, industry=:naics, level=4, verbose=true);
   @test size(unique(df_CBP_naics.naics_4), 1) > 50
   @test prod(map(x-> x ∈ names(df_CBP_naics),  
                  ["date_y", "msa", "naics_4", 
                   "payroll_by_fips", "emp_by_fips", "wage"]))
    # STATE
    df_CBP_naics = FirmDynamics.build_CBP_agg(1999:2001; 
                  aggregation=:state, industry=:naics, level=3, verbose=true);
   @test size(unique(df_CBP_naics.naics_3), 1) > 50
   @test prod(map(x-> x ∈ names(df_CBP_naics),  
                  ["date_y", "fipstate", "naics_3", 
                   "payroll_by_fips", "emp_by_fips", "wage"]))
   df_CBP_naics = FirmDynamics.build_CBP_agg(1999:2001; 
                  aggregation=:state, industry=:naics, level=4, verbose=true);
   @test size(unique(df_CBP_naics.naics_4), 1) > 50
   @test prod(map(x-> x ∈ names(df_CBP_naics),  
                  ["date_y", "fipstate", "naics_4", 
                   "payroll_by_fips", "emp_by_fips", "wage"]))
    # US
    df_CBP_naics = FirmDynamics.build_CBP_agg(2010:2011; 
                  aggregation=:us, industry=:naics, level=3, verbose=true);
   @test size(unique(df_CBP_naics.naics_3), 1) > 50
   @test prod(map(x-> x ∈ names(df_CBP_naics),  
                  ["date_y", "naics_3", 
                   "payroll_by_fips", "emp_by_fips", "wage"]))
   df_CBP_naics = FirmDynamics.build_CBP_agg(2010:2011; 
                  aggregation=:state, industry=:naics, level=4, verbose=true);
   @test size(unique(df_CBP_naics.naics_4), 1) > 50
   @test prod(map(x-> x ∈ names(df_CBP_naics),  
                  ["date_y", "naics_4", 
                   "payroll_by_fips", "emp_by_fips", "wage"]))


# --- TESTING ImportCES.jl
   df_CES_naics = FirmDynamics.download_CES();
   @test size(unique(df_CES_naics.year), 1) > 80
   df_CES_naics4 = build_CES(df_CES_naics,
                             industry=:naics, frequency=:annual, 
                             seasonal_adjustment=true, series_code=1, level=4)
   @test size(unique(df_CES_naics4.naics_4), 1) > 50
   @test prod(map(x-> x ∈ names(df_CES_naics4),  
                  ["date_y", "naics_4", "value",
                   "industry_code", "industry_name", "data_type_text", "series_id"]))
    df_CES_naics3 = build_CES(df_CES_naics,
                              industry=:naics, frequency=:annual, 
                              seasonal_adjustment=true, series_code=1, level=3)
   @test size(unique(df_CES_naics3.naics_3), 1) > 50
   @test prod(map(x-> x ∈ names(df_CES_naics3),  
                  ["date_y", "naics_3", "value",
                   "industry_code", "industry_name", "data_type_text", "series_id"]))
   df_CES_naics4 = build_CES(df_CES_naics,
                             industry=:naics, frequency=:monthly, 
                             seasonal_adjustment=true, series_code=1, level=4)
   @test size(unique(df_CES_naics4.naics_4), 1) > 50
   @test prod(map(x-> x ∈ names(df_CES_naics4),  
                  ["date", "naics_4", "value",
                   "industry_code", "industry_name", "data_type_text", "series_id"]))
    df_CES_naics3 = build_CES(df_CES_naics,
                              industry=:naics, frequency=:monthly, 
                              seasonal_adjustment=true, series_code=1, level=3)
   @test size(unique(df_CES_naics3.naics_3), 1) > 50
   @test prod(map(x-> x ∈ names(df_CES_naics3),  
                  ["date", "naics_3", "value",
                   "industry_code", "industry_name", "data_type_text", "series_id"]))


   df_CES_sic = FirmDynamics.download_CES_sic()
   @test size(unique(df_CES_sic.year), 1) >= 16
   df_CES_sic3 = build_CES_sic(df_CES_sic;
                               frequency=:annual, seasonal_adjustment=false,
                               series_code=4)   
   @test size(unique(df_CES_sic3.sic_3), 1) > 50
   @test prod(map(x-> x ∈ names(df_CES_sic3),  
                  ["date_y", "sic_3", "value", "data_type_text"]))
   df_CES_sic3 = build_CES_sic(df_CES_sic;
                               frequency=:monthly, seasonal_adjustment=false,
                               series_code=4)
   @test size(unique(df_CES_sic3.sic_3), 1) > 50
   @test prod(map(x-> x ∈ names(df_CES_sic3),  
                  ["date_m", "sic_3", "value", "data_type_text"]))
   
   
end
# ---------------------------------------------------------

