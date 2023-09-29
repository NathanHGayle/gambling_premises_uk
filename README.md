# Gambling Premises In The UK
Utilising publicly available data to see if there is a correlation between the socio-economic status of an area within the UK and the presence of gambling premises. One of the outputs should be a BI dashboard tool for communities to use in communication with their local authorities regarding the existence/opening of gambling premises. The second output should be a convenient dataset available on Kaggle to use for Data Science projects that can be categorised within a social awareness collection if appropriate.

# Steps

Null Replacement
  1. Webscraping data to replace nulls
  2. Manual replacements based on eyeballing through records & Google searching 
  3. Creating category for null premises activity

Deduplication
  1. ML dedupe model to identify possible duplicates
  2. Creating groupp field for matches and establishing official premises addresses
  3. API for activie/in_active
