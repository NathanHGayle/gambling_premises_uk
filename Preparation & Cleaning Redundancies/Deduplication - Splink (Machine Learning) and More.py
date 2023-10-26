import altair as alt
alt.renderers.enable('mimetype')
import pandas as pd
from splink.duckdb.duckdb_linker import DuckDBLinker
import splink.duckdb.duckdb_comparison_library as cl
from splink import charts
import splink

print('Running Splink version:', splink.__version__)
print('Creating new file to include IDs in NONULLs file...')
# Import dataset -- change method to connect to Git CSV in repository
df = pd.read_csv('Insert Path - Key Stage Downloads - CSVs\\NONULLS_premises-licence-register.csv')

df['unique_id'] = df['Flatten_ID']
print('Dataframe columns:', list(df.columns))

#Splink Time

settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        # "l.Account_Number = r.Account_Number",
        "l.Postcode = r.Postcode",
        # "l.Local_Authority = r.Local_Authority",
        "l.City = r.City",


    ],
    "comparisons": [
        cl.exact_match("Account_Number"),
        cl.levenshtein_at_thresholds("Account_Name", [1,2],include_exact_match_level=True),
        cl.levenshtein_at_thresholds("Premises_Activity",[1,2],include_exact_match_level=True),
        cl.exact_match("Local_Authority"),
        cl.levenshtein_at_thresholds("Address_Line_1",[1,2],include_exact_match_level=True),
        cl.levenshtein_at_thresholds("Address_Line_2",[1,2],include_exact_match_level=True),
        cl.exact_match("City"),
        cl.exact_match("Postcode"),
        cl.exact_match("Postcode_District"),
        cl.exact_match("Full_Address"),
        cl.exact_match("Adult_Gaming_Centre"),
        cl.exact_match("Betting_Shop"),
        cl.exact_match("Bingo"),
        cl.exact_match("Casino"),
        cl.exact_match("Casino_2005"),
        cl.exact_match("Family_Entertainment_Centre"),
        cl.exact_match("Other"),
        cl.exact_match("Pool_Betting")

    ],
}

linker = DuckDBLinker(df, settings)

analyse_columns = list(df.columns[2:])


# EDA
print('EDA STEP?: Do you want to save the profile chart in your environment, y/n?')
ans = input()
if ans == 'y':
    profile_cols = linker.profile_columns(analyse_columns, top_n=10, bottom_n=5)
    charts.save_offline_chart(profile_cols, filename="profilecols.html", overwrite=False)
else:
    pass

# Modelling
linker.load_settings(settings) # replace with linker.load_settings_from_json("my_settings.json") where appropriate
print ('Please choose a model 1) Estimate_u_using_random_sampling AND  estimate_parameters_using_expectation_maximisation 2) None, 1/2?')
ans1 = input()
if ans1 == '1':

    # Train M values
    blocking_rules_for_training = "l.City = r.City and l.Account_Name = r.Account_Name"
    training_session_an = linker.estimate_parameters_using_expectation_maximisation(blocking_rules_for_training) #fix_u_probabilities = False.
    linker.save_settings_to_json("expectation_maximisation.json", overwrite=True)
    m_u_chart = linker.m_u_parameters_chart()
    match_weights = linker.match_weights_chart()
    charts.save_offline_chart(m_u_chart, filename="m_u_chart_from_label.html", overwrite=True)
    charts.save_offline_chart(match_weights, filename="match_weights_from_label.html", overwrite=True)

    # Train M values again
    blocking_rules_for_training_two = "l.Postcode = r.Postcode"
    training_session_two = linker.estimate_parameters_using_expectation_maximisation(blocking_rules_for_training_two) #fix_u_probabilities = False.
    linker.save_settings_to_json("expectation_maximisation_two.json", overwrite=True)
    m_u_chart_two = linker.m_u_parameters_chart()
    match_weights_two = linker.match_weights_chart()
    charts.save_offline_chart(m_u_chart, filename="m_u_chart_two_from_label.html", overwrite=True)
    charts.save_offline_chart(match_weights, filename="match_weights_two_from_label.html", overwrite=True)

    # Train U values
    linker.estimate_u_using_random_sampling(1e8)
    linker.save_settings_to_json("u_from_random_sample.json", overwrite=True)
    m_u_chart = linker.m_u_parameters_chart()
    match_weights = linker.match_weights_chart()
    charts.save_offline_chart(m_u_chart, filename="m_u_chart_u_random.html", overwrite=True)
    charts.save_offline_chart(match_weights, filename="match_weights_u_random.html", overwrite=True)

else:
    print("You are presuming your model is already trained, continuing on to predictions...")

print('Now we are going to predict based on a 0.2 threshold match probability...')


e = linker.predict(threshold_match_probability= 0.2)
df_e = e.as_pandas_dataframe()
df_e.to_csv('Insert Path - Splink - ML for Deduplication\\Duplication CSV\\Match_Weight_DataFrame.csv', index=True)

print(df_e)

records_to_plot = df_e.head().to_dict(orient= "read")
waterfall = charts.waterfall_chart(records_to_plot,linker._settings_obj_,filter_nulls=False)
charts.save_offline_chart(waterfall, filename="waterfall_chart.html", overwrite=True)

precision_recalls = linker.precision_recall_chart_from_labels_column("Flatten_ID")
charts.save_offline_chart(precision_recalls, filename="precision_recalls.html", overwrite=True)

print('Successful')

