-- Table names are specified using ' -- ' before each 'Let' statement. This code
was obtained from Power BI's Power Query Advanced Editor.


-- UK_gambling_premises_social_class_and_deprivation

let
    Source = Csv.Document(File.Contents("C:\Users\natha\PycharmProjects\gambling_premises_in_the_uk\Preparation & Cleaning Redundancies\Key Stage Downloads - CSVs\Final20_01_2024_T2202The_UK_gambling_premises_social_class_and_deprivation_dataset.csv"),[Delimiter=",", Columns=71, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"", Int64.Type}, {"Unnamed: 0", Int64.Type}, {"Flatten_ID", type text}, {"Account_Number", type text}, {"Account_Name", type text}, {"Premises_Activity", type text}, {"Local_Authority", type text}, {"Address_Line_1", type text}, {"Address_Line_2", type text}, {"City", type text}, {"Postcode", type text}, {"Postcode_District", type text}, {"Full_Address", type text}, {"Adult_Gaming_Centre_prem_activity", Int64.Type}, {"Betting_Shop_prem_activity", Int64.Type}, {"Bingo_prem_activity", Int64.Type}, {"Casino_prem_activity", Int64.Type}, {"Casino_2005_prem_activity", Int64.Type}, {"Family_Entertainment_Centre_prem_activity", Int64.Type}, {"Other_prem_activity", Int64.Type}, {"Pool_Betting_prem_activity", Int64.Type}, {"google_api_result", type text}, {"business_status_google_api_", type text}, {"formatted_address_google_api_", type text}, {"place_id_google_api_", type text}, {"yyyy_mm_dd_google_api", type date}, {"postcode_single", type text}, {"postcode_district_single", type text}, {"extracted_postcode", type text}, {"extracted_district", type text}, {"google_api_correction", type logical}, {"google_district_check", type logical}, {"original_address", type text}, {"Postcode_postcode_data", type text}, {"In Use?_postcode_data", type text}, {"Latitude_postcode_data", type number}, {"Longitude_postcode_data", type number}, {"Constituency_postcode_data", type text}, {"Population_postcode_data", Int64.Type}, {"Households_postcode_data", Int64.Type}, {"count_occurances_postcode_data", type text}, {"fill_constituency_webscrape", type text}, {"postcode_inputs_webscrape", type text}, {"Population_by_Constituency_postcode_data", Int64.Type}, {"scaled_population_by_constituency_postcode_data", type number}, {"Total_Premises_by_Constituency", Int64.Type}, {"Total_Operational_Premises_by_Constituency", Int64.Type}, {"ConstituencyName_nsec_data", type text}, {"Managerial, administrative and professional occupations", type number}, {"Intermediate occupations", type number}, {"Routine and manual occupations", type number}, {"Full-time students", type number}, {"Never worked / long-term unemployed", type number}, {"Total Population By Constituency_nsec_calculated", type number}, {"ConstituencyName_id_2019_data", type text}, {"DateOfThisUpdate_id_2019_data", type date}, {"DateOfDataset_id_2019_data", type date}, {"IMD rank 2019_id_2019_data", Int64.Type}, {"IMD rank 2015_id_2019_data", Int64.Type}, {"Change in rank since 2015_id_2019_data", Int64.Type}, {"Number of LSOAs in most deprived decile_id_2019_data", Int64.Type}, {"Share of LSOAs in most deprived decile_id_2019_data", Percentage.Type}, {"Income_id_2019_data", Int64.Type}, {"Employment_id_2019_data", Int64.Type}, {"Education, skills and training_id_2019_data", Int64.Type}, {"Health deprivation and disability_id_2019_data", Int64.Type}, {"Crime_id_2019_data", Int64.Type}, {"Barriers to housing and services_id_2019_data", Int64.Type}, {"Living environment_id_2019_data", Int64.Type}, {"IDACI_id_2019_data", Int64.Type}, {"IDAOPI_id_2019_data", Int64.Type}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Changed Type", "Constituency_postcode_data", "Constituency_postcode_data - Copy"),
    #"Renamed Columns" = Table.RenameColumns(#"Duplicated Column",{{"Constituency_postcode_data - Copy", "lower_Constituency_key"}}),
    #"Lowercased Text" = Table.TransformColumns(#"Renamed Columns",{{"lower_Constituency_key", Text.Lower, type text}}),
    #"Trimmed Text" = Table.TransformColumns(#"Lowercased Text",{{"lower_Constituency_key", Text.Trim, type text}}),
    #"Cleaned Text" = Table.TransformColumns(#"Trimmed Text",{{"lower_Constituency_key", Text.Clean, type text}}),
    constituency_key = Table.ReplaceValue(#"Cleaned Text"," ","",Replacer.ReplaceText,{"lower_Constituency_key"})
in
    constituency_key

-- ID_2019

    let
    Source = UK_gambling_premises_social_class_and_deprivation,
    #"Grouped Rows" = Table.Group(Source, {"lower_Constituency_key"}, {{"Crime_id_2019_data", each List.Max([Crime_id_2019_data]), type nullable number}, {"Employment_id_2019_data", each List.Max([Employment_id_2019_data]), type nullable number}, {"Barriers to housing and services_id_2019_data", each List.Max([Barriers to housing and services_id_2019_data]), type nullable number}, {"Education skills and training_id_2019_data", each List.Max([#"Education, skills and training_id_2019_data"]), type nullable number}, {"Health deprivation and disability_id_2019_data", each List.Max([Health deprivation and disability_id_2019_data]), type nullable number}, {"Income_id_2019_data", each List.Max([Income_id_2019_data]), type nullable number}, {"Living environment_id_2019_data", each List.Max([Living environment_id_2019_data]), type nullable number}}),
    #"Added Custom" = Table.AddColumn(#"Grouped Rows", "IDM_Min Value", each List.Min({[Crime_id_2019_data],[Employment_id_2019_data],[Barriers to housing and services_id_2019_data],[Education skills and training_id_2019_data],[Health deprivation and disability_id_2019_data],[Income_id_2019_data],[Living environment_id_2019_data]})),
    #"Unpivoted Columns" = Table.UnpivotOtherColumns(#"Added Custom", {"lower_Constituency_key", "IDM_Min Value"}, "Attribute", "Value"),
    #"Renamed Columns1" = Table.RenameColumns(#"Unpivoted Columns",{{"Attribute", "IMD_Group"}, {"Value", "IMD_Constituency_Pop"}}),
    #"Replaced Value" = Table.ReplaceValue(#"Renamed Columns1","_id_2019_data","",Replacer.ReplaceText,{"IMD_Group"}),
    #"Added Custom1" = Table.AddColumn(#"Replaced Value", "IMD Group Min", each if [IDM_Min Value] = [IMD_Constituency_Pop] then [IMD_Group] else null),
    #"Changed Type1" = Table.TransformColumnTypes(#"Added Custom1",{{"IDM_Min Value", Int64.Type}})
in
    #"Changed Type1"

-- NSEC

    let
    Source = UK_gambling_premises_social_class_and_deprivation,
    #"Grouped Rows" = Table.Group(Source, {"lower_Constituency_key"}, {{"Managerial, administrative and professional occupations", each List.Max([#"Managerial, administrative and professional occupations"]), type nullable number}, {"Intermediate occupations", each List.Max([Intermediate occupations]), type nullable number}, {"Routine and manual occupations", each List.Max([Routine and manual occupations]), type nullable number}, {"Full-time students", each List.Max([#"Full-time students"]), type nullable number}, {"Never worked / long-term unemployment", each List.Max([#"Never worked / long-term unemployed"]), type nullable number}}),
    #"Inserted Maximum" = Table.AddColumn(#"Grouped Rows", "Maximum", each List.Max({[#"Managerial, administrative and professional occupations"], [Intermediate occupations], [Routine and manual occupations], [#"Full-time students"], [#"Never worked / long-term unemployment"]}), type number),
    #"Unpivoted Columns1" = Table.UnpivotOtherColumns(#"Inserted Maximum", {"lower_Constituency_key", "Maximum"}, "Attribute", "Value"),
    #"Reordered Columns" = Table.ReorderColumns(#"Unpivoted Columns1",{"lower_Constituency_key", "Attribute", "Value", "Maximum"}),
    #"Renamed Columns" = Table.RenameColumns(#"Reordered Columns",{{"Attribute", "NSEC_Group"}, {"Value", "NSEC_ConstituencyPopulation"}})
in
    #"Renamed Columns"

-- Constituency_LKP

    let
    Source = UK_gambling_premises_social_class_and_deprivation,
    #"Grouped Rows" = Table.Group(Source, {"lower_Constituency_key", "Constituency_postcode_data"}, {{"Null", each Table.RowCount(Table.Distinct(_)), Int64.Type}}),
    #"Removed Duplicates" = Table.Distinct(#"Grouped Rows", {"lower_Constituency_key"}),
    #"Removed Columns" = Table.RemoveColumns(#"Removed Duplicates",{"Null"})
in
    #"Removed Columns"

-- Constituency_PostCode_District_LKP

let
    Source = UK_gambling_premises_social_class_and_deprivation,
    #"Grouped Rows" = Table.Group(Source, {"lower_Constituency_key", "Postcode_District"}, {{"Null", each Table.RowCount(Table.Distinct(_)), Int64.Type}}),
    #"Sorted Rows" = Table.Sort(#"Grouped Rows",{{"lower_Constituency_key", Order.Descending}}),
    #"Removed Columns" = Table.RemoveColumns(#"Sorted Rows",{"Null"})
in
    #"Removed Columns"

-- Total_Premesis

    let
    Source = UK_gambling_premises_social_class_and_deprivation,
    #"Operational" = Table.SelectRows(Source, each ([business_status_google_api_] = "OPERATIONAL")),
    #"Grouped Rows" = Table.Group(Operational, {"lower_Constituency_key", "business_status_google_api_"}, {{"Total Premesis", each Table.RowCount(Table.Distinct(_)), Int64.Type}, {"Population", each List.Sum([Population_postcode_data]), type nullable number}}),
    #"Pivoted Column" = Table.Pivot(#"Grouped Rows", List.Distinct(#"Grouped Rows"[business_status_google_api_]), "business_status_google_api_", "Total Premesis", List.Sum),
    #"Total Operation Rename" = Table.RenameColumns(#"Pivoted Column",{{"OPERATIONAL", "Total Operational Premises"}}),
    PPC = Table.AddColumn(#"Total Operation Rename", "PPC_Perc", each if [Population] <> 0 then
        Number.Round([Total Operational Premises] / [Population], 2)
    else
        null),
    #"Changed Type1" = Table.TransformColumnTypes(PPC,{{"PPC_Perc", type number}}),
    #"Sorted Rows" = Table.Sort(#"Changed Type1",{{"PPC_Perc", Order.Ascending}}),
    #"Removed Null Cons (no pop)" = Table.SelectRows(#"Sorted Rows", each ([PPC_Perc] <> null)),
    #"Added Index" = Table.AddIndexColumn(#"Removed Null Cons (no pop)", "PPC Rank", 1, 1, Int64.Type)
in
    #"Added Index"

-- op_spearman_matrix

    let
    Source = Csv.Document(File.Contents("C:\Users\natha\PycharmProjects\gambling_premises_in_the_uk\Preparation & Cleaning Redundancies\Key Stage Downloads - CSVs\op_spearman_matrix.csv"),[Delimiter=",", Columns=26, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"", type text}, {"Adult_Gaming_Centre_prem_activity", type number}, {"Betting_Shop_prem_activity", type number}, {"Bingo_prem_activity", type number}, {"Casino_prem_activity", type number}, {"Casino_2005_prem_activity", type number}, {"Family_Entertainment_Centre_prem_activity", type number}, {"Other_prem_activity", type number}, {"Pool_Betting_prem_activity", type number}, {"Total_Premises_by_Constituency", type number}, {"Total_Operational_Premises_by_Constituency", type number}, {"Managerial, administrative and professional occupations", type number}, {"Intermediate occupations", type number}, {"Routine and manual occupations", type number}, {"Full-time students", type number}, {"Never worked / long-term unemployed", type number}, {"IMD rank 2019_id_2019_data", type number}, {"IMD rank 2015_id_2019_data", type number}, {"Number of LSOAs in most deprived decile_id_2019_data", type number}, {"Income_id_2019_data", type number}, {"Employment_id_2019_data", type number}, {"Education, skills and training_id_2019_data", type number}, {"Health deprivation and disability_id_2019_data", type number}, {"Crime_id_2019_data", type number}, {"Barriers to housing and services_id_2019_data", type number}, {"Living environment_id_2019_data", type number}}),
    #"Replaced id_2019:" = Table.ReplaceValue(#"Changed Type","_id_2019_data","",Replacer.ReplaceText,{""})
in
    #"Replaced id_2019:"

-- Premises Activity

    let
    Source = UK_gambling_premises_social_class_and_deprivation,
    #"Unpivoted Columns" = Table.UnpivotOtherColumns(Source, {"", "Unnamed: 0", "Flatten_ID", "Account_Number", "Account_Name", "Premises_Activity", "Local_Authority", "Address_Line_1", "Address_Line_2", "City", "Postcode", "Postcode_District", "Full_Address", "Pool_Betting_prem_activity", "google_api_result", "business_status_google_api_", "formatted_address_google_api_", "place_id_google_api_", "yyyy_mm_dd_google_api", "postcode_single", "postcode_district_single", "extracted_postcode", "extracted_district", "google_api_correction", "google_district_check", "original_address", "Postcode_postcode_data", "In Use?_postcode_data", "Latitude_postcode_data", "Longitude_postcode_data", "Constituency_postcode_data", "Population_postcode_data", "Households_postcode_data", "count_occurances_postcode_data", "fill_constituency_webscrape", "postcode_inputs_webscrape", "Population_by_Constituency_postcode_data", "scaled_population_by_constituency_postcode_data", "Total_Premises_by_Constituency", "Total_Operational_Premises_by_Constituency", "ConstituencyName_nsec_data", "Managerial, administrative and professional occupations", "Intermediate occupations", "Routine and manual occupations", "Full-time students", "Never worked / long-term unemployed", "Total Population By Constituency_nsec_calculated", "ConstituencyName_id_2019_data", "DateOfThisUpdate_id_2019_data", "DateOfDataset_id_2019_data", "IMD rank 2019_id_2019_data", "IMD rank 2015_id_2019_data", "Change in rank since 2015_id_2019_data", "Number of LSOAs in most deprived decile_id_2019_data", "Share of LSOAs in most deprived decile_id_2019_data", "Income_id_2019_data", "Employment_id_2019_data", "Education, skills and training_id_2019_data", "Health deprivation and disability_id_2019_data", "Crime_id_2019_data", "Barriers to housing and services_id_2019_data", "Living environment_id_2019_data", "IDACI_id_2019_data", "IDAOPI_id_2019_data", "lower_Constituency_key"}, "Attribute", "Value"),
    #"Grouped Rows" = Table.Group(#"Unpivoted Columns", {"lower_Constituency_key", "Attribute"}, {{"Total", each List.Sum([Value]), type number}, {"Total Premises", each Table.RowCount(Table.Distinct(_)), Int64.Type}}),
    #"Renamed Columns" = Table.RenameColumns(#"Grouped Rows",{{"Attribute", "Premises Activity"}}),
    #"Replaced Value" = Table.ReplaceValue(#"Renamed Columns","_prem_activity","",Replacer.ReplaceText,{"Premises Activity"}),
    #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value","_"," ",Replacer.ReplaceText,{"Premises Activity"}),
    #"Replaced Value2" = Table.ReplaceValue(#"Replaced Value1","Casino 2005","Casino",Replacer.ReplaceText,{"Premises Activity"}),
    #"Trimmed Text" = Table.TransformColumns(#"Replaced Value2",{{"Premises Activity", Text.Trim, type text}}),
    #"Capitalized Each Word" = Table.TransformColumns(#"Trimmed Text",{{"Premises Activity", Text.Proper, type text}}),
    #"Filtered Rows" = Table.SelectRows(#"Capitalized Each Word", each ([Total] <> 0))
in
    #"Filtered Rows"