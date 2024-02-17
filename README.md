# Gambling Premises In The UK
Analysing the correlations between the UK gambling premises, National Socio-Economic Class and Deprivation by Constituency. 
This code powers a publicly available Power BI dashboard serving as a tool for communities to use in communication with
their local councils available here:
[UK Gambling Premises, NSEC, and Deprivation Dashboard](https://app.powerbi.com/view?r=eyJrIjoiY2ZiZTU2MTUtMjk0OS00ZDJiLWEwMGItNzZiYzg3YTYzMjI5IiwidCI6IjgyMmRkYmEwLWFkNjAtNDE2Zi1iNDRlLTEwMzdlNzRkNTI5OSJ9)

# Steps

NaN Replacement
  1. Webscraping Locla Council information to replace NaNs in ``` `python
     df['Local Authority'] ```
  3. Manually finding and replacing NaNs in the ``` `python
     df['Postcode'] ```.  
  5. Creating an 'Other' category for NaNs in ``` `python
     df['Premises Activity'] ```

Reshaping
  1. String refinements and case handling
  2. Deduplication through transformation techniques
  3. Running Splink to identify less obvious duplicatations -- file not included in repository yet.

API calls
  1. Calling Google Maps API to surface the Business Status of Gambling Premises, e.g 'OPERATIONAL', 'PERMINANTLY CLOSED' etc. 
  2. Adding numerical metrics for Indicies of Deprivation 2019 and National Soci-Economic Class for correlation analysis.

Analysis
  1. The Spearman correlation method was selected to handle anomalies and thus better represent the raw scatter plot visualisation in the Power BI dashboard.
  2. The Power BI dashboard product aims to bring awareness to these correlation trends and increase visibility of gamling premises across constituencies.  
  
  
