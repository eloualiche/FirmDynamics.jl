using FirmDynamics
using Test
# ---------------------------------------------------------


# ---------------------------------------------------------
@testset "FirmDynamics.jl" begin

# --- testing the tests
    # @test FirmDynamics.greet_your_package_name() == "Hello YourPackageName!"
    # @test FirmDynamics.greet_your_package_name() != "Hello world!"
    

# --- testing of the SIC files 
    # df_CBP_sic     = build_CBP(1983:2020, aggregation=:county, industry=:sic);
    # df_CBP_emp_sic2_v1 = build_emp_CBP(1983:2020, aggregation=:county, industry=:sic, level=2);
    # df_CBP_emp_sic2_v2 = build_emp_CBP(df_CBP_sic, aggregation=:county, industry=:sic, level=2);
    # @test df_CBP_emp_sic2_v1.emp_by_fips == df_CBP_emp_sic2_v2.emp_by_fips

# --- testing the naics files
    # df_CBP_naics = FirmDynamics.build_CBP(
    #     1985:2022, aggregation=:county, industry=:naics);
    # df_CBP_emp_naics4_v1 = FirmDynamics.build_emp_CBP(
    #     1985:2022, aggregation=:county, industry=:naics, level=4);
    # df_CBP_emp_naics4_v2 = FirmDynamics.build_emp_CBP(
    #     df_CBP_naics, aggregation=:county, industry=:naics, level=4);
    # @test df_CBP_emp_naics4_v1.emp_by_fips == df_CBP_emp_naics4_v2.emp_by_fips;

end
# ---------------------------------------------------------

