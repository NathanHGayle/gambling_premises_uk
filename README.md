# Gambling Premises In The UK
Analysing the correlations between the UK gambling premises, national socio-economic class and deprivation by Constituency. This code powers the Power BI dashboard and the objective is for this to be a tool for communities to use in communication with their local councils with regards to the existence/opening of gambling premises.

# Steps

Null Replacement
  1. Webscraping data to replace nulls in Local Councils, and Postcodes.
  2. Manual replacements based on eyeballing through records & Google searching 
  3. Creating a default category for null premises activity

Reshaping
  1. String refinements and case handling
  2. Deduplication through transformation techniques
  3. Running ML dedupe model to identify less obvious duplicatations

API calls
  1. Calling an API to categorise fields based on locational data
  2. Adding numerical metrics from reliable sources for correlation analysis

Analysis
  1. Correlation method selected to handle anomalies 
  2. Dashboard visualises insights most useful for bringing awareness to trends and rankings.  
  
  
