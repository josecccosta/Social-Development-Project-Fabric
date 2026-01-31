[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "other human_development_index_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ID", DestinationColumnName = "ID"], [SourceColumnName = "Country_Code_Iso3", DestinationColumnName = "Country_Code_Iso3"], [SourceColumnName = "Year", DestinationColumnName = "Year"], [SourceColumnName = "Human_Development_Index", DestinationColumnName = "Human_Development_Index"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"other human_development_index" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "other.Human_Development_Index", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"Code", "Country_Code"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"Entity"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"Country_Code", "Code_Iso3"}}),
  #"Filtered rows" = Table.SelectRows(#"Renamed columns 1", each ([Code_Iso3] <> null)),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows", {"World_region_according_to_OWID"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed columns 1", each ([Code_Iso3] <> "OWID_AUH" and [Code_Iso3] <> "OWID_CZS" and [Code_Iso3] <> "OWID_ERE" and [Code_Iso3] <> "OWID_GDR" and [Code_Iso3] <> "OWID_GFR" and [Code_Iso3] <> "OWID_KOS" and [Code_Iso3] <> "OWID_KRU" and [Code_Iso3] <> "OWID_RVN" and [Code_Iso3] <> "OWID_SRM" and [Code_Iso3] <> "OWID_USS" and [Code_Iso3] <> "OWID_WRL" and [Code_Iso3] <> "OWID_YAR" and [Code_Iso3] <> "OWID_YGS" and [Code_Iso3] <> "OWID_YPR")),
  #"Added index" = Table.AddIndexColumn(#"Filtered rows 1", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Code_Iso3", "Year", "Human_Development_Index"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Reordered columns", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns 2", {"ID"}, false),
  #"Renamed columns 3" = Table.RenameColumns(#"Marked key columns", {{"Code_Iso3", "Country_Code_Iso3"}}),
  #"Filtered rows 2" = Table.SelectRows(#"Renamed columns 3", each ([Human_Development_Index] <> null))
in
  #"Filtered rows 2";
shared #"other income_share_distribution" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "other.Income_Share_Distribution", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([Code] <> null and [Code] <> "OWID_GDR" and [Code] <> "OWID_KOS" and [Code] <> "OWID_WRL")),
  #"Removed columns" = Table.RemoveColumns(#"Filtered rows", {"Entity"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Share__top_1%__before_tax_", "Share_Top_1_pct"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"Share__90-99th_percentiles__before_tax_"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns 1", {{"Share__middle_40%__before_tax_", "Share_Middle_40_pct"}, {"Share__bottom_50%__before_tax_", "Share_Bottom_50_pct"}, {"Code", "Country_Code_Iso3"}}),
  #"Added index" = Table.AddIndexColumn(#"Renamed columns 1", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Country_Code_Iso3", "Year", "Share_Top_1_pct", "Share_Middle_40_pct", "Share_Bottom_50_pct"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Reordered columns", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns 2", {"ID"}, false)
in
  #"Marked key columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "other income_share_top_10_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"other income_share_top_10" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "other.Income_Share_Top_10", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Entity"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Code", "Country_Code_Iso3"}, {"Share__top_10%__before_tax_", "Share_Top_10_pct"}}),
  #"Added index" = Table.AddIndexColumn(#"Renamed columns", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Country_Code_Iso3", "Year", "Share_Top_10_pct"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns", {{"Index", "ID"}}),
  #"Filtered rows" = Table.SelectRows(#"Renamed columns 1", each ([Country_Code_Iso3] <> null and [Country_Code_Iso3] <> "OWID_GDR" and [Country_Code_Iso3] <> "OWID_KOS" and [Country_Code_Iso3] <> "OWID_WRL")),
  #"Marked key columns" = Table.AddKey(#"Filtered rows", {"ID"}, false),
  #"Merged queries" = Table.NestedJoin(#"Marked key columns", {"Country_Code_Iso3", "Year"}, #"other income_share_distribution", {"Country_Code_Iso3", "Year"}, "other income_share_distribution", JoinKind.FullOuter),
  #"Expanded other income_share_distribution" = Table.ExpandTableColumn(#"Merged queries", "other income_share_distribution", {"Share_Top_1_pct", "Share_Middle_40_pct", "Share_Bottom_50_pct"}, {"Share_Top_1_pct", "Share_Middle_40_pct", "Share_Bottom_50_pct"})
in
  #"Expanded other income_share_distribution";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "other multidimensional_poverty_index_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"other multidimensional_poverty_index" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "other.Multidimensional_Poverty_Index", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Entity"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Code", "Country_Code_Iso3"}, {"Multidimensional_Poverty_Index__MPI___National__-_Harmonized_over_time", "MPI"}}),
  #"Added index" = Table.AddIndexColumn(#"Renamed columns", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Country_Code_Iso3", "Year", "MPI"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns 1", {"ID"}, false)
in
  #"Marked key columns";
shared #"un_census attainment_15plus" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "un_census.Attainment_15Plus", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Table_Code"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Country_or_Area_Code", "Country_Code_Numeric"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"Country_or_Area", "Reference_Date", "Area", "Sex"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns 1", each ([Age] <> null and [Age] <> "Unknown")),
  #"Removed columns 2" = Table.RemoveColumns(#"Filtered rows", {"Educational_attainment", "Record_Type_Code", "Record_Type", "Reliability_Code", "Reliability", "Source_Year", "Value_Footnotes"}),
  #"Added index" = Table.AddIndexColumn(#"Removed columns 2", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Country_Code_Numeric", "Year", "Area_Code", "Sex_Code", "Code_of_Age", "Age", "Code_of_Educational_attainment", "Value"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns 1", {"ID"}, false),
  #"Changed column type" = Table.TransformColumnTypes(#"Marked key columns", {{"Country_Code_Numeric", Int64.Type}})
in
  #"Changed column type";
shared #"un_census employed_population_by_status_in_employment_age_and_sex" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "un_census.Employed_Population_By_Status_In_Employment_Age_And_Sex", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Table_Code"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Country_or_Area_Code", "Country_Code_Numeric"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"Country_or_Area", "Reference_Date", "Area", "Sex", "Status_in_employment", "Record_Type_Code", "Record_Type", "Reliability_Code", "Reliability", "Source_Year", "Value_Footnotes"}),
  #"Added index" = Table.AddIndexColumn(#"Removed columns 1", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Country_Code_Numeric", "Year", "Area_Code", "Sex_Code", "Age", "Code_of_Status_in_employment", "Value"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns 1", {"ID"}, false),
  #"Filtered rows" = Table.SelectRows(#"Marked key columns", each ([Year] <> null) and ([Age] <> "Unknown")),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows", {{"Country_Code_Numeric", Int64.Type}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "un_census gini_index_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ID", DestinationColumnName = "ID"], [SourceColumnName = "Country_Code_Iso3", DestinationColumnName = "Country_Code_Iso3"], [SourceColumnName = "Year", DestinationColumnName = "Year"], [SourceColumnName = "Value", DestinationColumnName = "Value"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"un_census gini_index" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "un_census.Gini_Index", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Indicator_Code", "Country_or_Area", "Time_Code", "Value_Footnotes"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Country_or_Area_Code", "Country_Code_Iso3"}}),
  #"Added index" = Table.AddIndexColumn(#"Renamed columns", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Country_Code_Iso3", "Year", "Value"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns 1", {"ID"}, false),
  #"Filtered rows" = Table.SelectRows(#"Marked key columns", each ([Value] <> null))
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "un_census monthly_employee_earnings_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"un_census monthly_employee_earnings" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "un_census.Monthly_Employee_Earnings", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"source.label", "indicator.label"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each ([classif1.label] <> "Currency: 2021 PPP $")),
  #"Renamed columns" = Table.RenameColumns(#"Filtered rows", {{"time", "Year"}, {"obs_value", "value"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"note_source.label", "note_indicator.label", "note_classif.label", "obs_status.label"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns 1", {{"sex.label", "sex"}}),
  #"Inserted conditional column" = Table.AddColumn(#"Renamed columns 1", "Sex_Code", each if [sex] = "Total" then 0 else if [sex] = "Female" then 2 else if [sex] = "Male" then 1 else ""),
  #"Reordered columns" = Table.ReorderColumns(#"Inserted conditional column", {"ref_area.label", "sex", "Sex_Code", "classif1.label", "Year", "value"}),
  #"Removed columns 2" = Table.RemoveColumns(#"Reordered columns", {"sex"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 2", {{"classif1.label", "Label"}, {"value", "Value"}}),
  #"Added index" = Table.AddIndexColumn(#"Renamed columns 2", "Index", 0, 1, Int64.Type),
  #"Reordered columns 1" = Table.ReorderColumns(#"Added index", {"Index", "ref_area.label", "Sex_Code", "Label", "Year", "Value"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Reordered columns 1", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns 3", {"ID"}, false),
  #"Changed column type" = Table.TransformColumnTypes(#"Marked key columns", {{"Sex_Code", Int64.Type}}),
  #"Renamed columns 4" = Table.RenameColumns(#"Changed column type", {{"ref_area.label", "Country_Area"}}),
  #"Merged queries" = Table.NestedJoin(#"Renamed columns 4", {"Country_Area"}, #"Reference_Database geography_dimension", {"country_or_area"}, "Reference_Database geography_dimension", JoinKind.Inner),
  #"Expanded Reference_Database geography_dimension" = Table.ExpandTableColumn(#"Merged queries", "Reference_Database geography_dimension", {"Country_Code_Numeric"}, {"Country_Code_Numeric"}),
  #"Reordered columns 2" = Table.ReorderColumns(#"Expanded Reference_Database geography_dimension", {"ID", "Country_Code_Numeric", "Country_Area", "Sex_Code", "Label", "Year", "Value"}),
  #"Removed columns 3" = Table.RemoveColumns(#"Reordered columns 2", {"Country_Area"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed columns 3", each ([Sex_Code] = 0) and ([Label] = "Currency: U.S. dollars")),
  #"Removed columns 4" = Table.RemoveColumns(#"Filtered rows 1", {"Sex_Code", "Label"})
in
  #"Removed columns 4";
shared #"un_census pop_age_sex_urban_rural" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "un_census.Pop_Age_Sex_Urban_Rural", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Table_Code", "Country_or_Area", "Reference_Date", "Area", "Sex", "Record_Type_Code", "Record_Type", "Reliability_Code", "Reliability", "Source_Year", "Value_Footnotes"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each Text.Contains([Age], "Total") or Text.Contains([Age], "-")),
  #"Renamed columns" = Table.RenameColumns(#"Filtered rows", {{"Country_or_Area_Code", "Country_Code_Numeric"}}),
  #"Added index" = Table.AddIndexColumn(#"Renamed columns", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Country_Code_Numeric", "Year", "Area_Code", "Sex_Code", "Code_of_Age", "Age", "Value"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns 1", {"ID"}, false),
  #"Changed column type" = Table.TransformColumnTypes(#"Marked key columns", {{"Country_Code_Numeric", Int64.Type}})
in
  #"Changed column type";
shared #"un_census school_attendance" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "un_census.School_Attendance", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Table_Code", "Country_or_Area", "Reference_Date", "Area", "Sex", "School_attendance", "Record_Type_Code", "Record_Type", "Reliability_Code", "Reliability", "Source_Year", "Value_Footnotes"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Country_or_Area_Code", "Country_Code_Numeric"}}),
  #"Added index" = Table.AddIndexColumn(#"Renamed columns", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Country_Code_Numeric", "Year", "Area_Code", "Sex_Code", "Code_of_Age", "Age", "Code_of_School_attendance", "Value"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns 1", {"ID"}, false),
  #"Filtered rows" = Table.SelectRows(#"Marked key columns", each ([Age] <> null and [Age] <> "Unknown")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each Text.Contains([Age], "-")),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows 1", {{"Country_Code_Numeric", Int64.Type}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Reference_Database geography_dimension_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "region_name", DestinationColumnName = "region_name"], [SourceColumnName = "sub-region_name", DestinationColumnName = "sub-region_name"], [SourceColumnName = "intermediate_region_name", DestinationColumnName = "intermediate_region_name"], [SourceColumnName = "country_or_area", DestinationColumnName = "country_or_area"], [SourceColumnName = "Country_Code_Numeric", DestinationColumnName = "Country_Code_Numeric"], [SourceColumnName = "Country_Code_Iso2", DestinationColumnName = "Country_Code_Iso2"], [SourceColumnName = "Country_Code_Iso3", DestinationColumnName = "Country_Code_Iso3"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"Reference_Database geography_dimension" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Reference_Database.Geography_Dimension", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"m49_code", "country_code_numeric"}, {"iso-alpha2_code", "country_code_iso2"}, {"iso-alpha3_code", "country_code_iso3"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"least_developed_countries_ldc", "land_locked_developing_countries_lldc", "small_island_developing_states_sids"}),
  #"Marked key columns" = Table.AddKey(#"Removed columns", {"country_code_numeric"}, false),
  #"Marked key columns 1" = Table.AddKey(#"Marked key columns", {"country_code_iso2"}, false),
  #"Marked key columns 2" = Table.AddKey(#"Marked key columns 1", {"country_code_iso3"}, false),
  #"Renamed columns 1" = Table.RenameColumns(#"Marked key columns 2", {{"country_code_numeric", "Country_Code_Numeric"}, {"country_code_iso2", "Country_Code_Iso2"}, {"country_code_iso3", "Country_Code_Iso3"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"global_code", "global_name", "region_code", "sub-region_code", "intermediate_region_code"})
in
  #"Removed columns 1";
shared #"other multidimensional_poverty_index_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "MPI", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Reference_Database area_dimension" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Reference_Database.Area_Dimension", ItemKind = "Table"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"Area_Code"}, false)
in
  #"Marked key columns";
shared #"Reference_Database attainment_status_dimension" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Reference_Database.Attainment_Status_Dimension", ItemKind = "Table"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"Code_of_Educational_attainment"}, false)
in
  #"Marked key columns";
shared #"Reference_Database employment_status_dimension" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Reference_Database.Employment_Status_Dimension", ItemKind = "Table"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"Code_of_Status_in_employment"}, false)
in
  #"Marked key columns";
shared #"Reference_Database school_attendance_dimension" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Reference_Database.School_Attendance_Dimension", ItemKind = "Table"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"Code_of_School_attendance"}, false)
in
  #"Marked key columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Reference_Database gender_dimension_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Sex_Code", DestinationColumnName = "Sex_Code"], [SourceColumnName = "Sex", DestinationColumnName = "Sex"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"Reference_Database gender_dimension" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Reference_Database.Gender_Dimension", ItemKind = "Table"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"Sex_Code"}, false)
in
  #"Marked key columns";
shared #"Reference_Database gender_dimension_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Gender", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared #"Reference_Database geography_dimension_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Geography", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared #"un_census gini_index_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Gini_Index", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared #"other human_development_index_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "HDI", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared #"other income_share_top_10_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Income_Share", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"un_census monthly_employee_earnings_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Monthly_Employee_Earnings", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
