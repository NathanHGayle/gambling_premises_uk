# The Link Between Gambling Premises, Socio-Economic Class, and Deprivation
This repository maintains the data pipeline powering a publicly available Deepnote report for local communities to review with their councils.

## Overview 

### Findings
The correlations are problematic, and arguements have been made that they point to a lack of power held by the UK's local councils. Share the report with friends and family, and look into active campaigns listed below.

### Make A Difference
* [Remind London Mayor of TFL Gambling Ads Ban](https://you.38degrees.org.uk/petitions/stop-bombarding-us-with-gambling-ads-on-public-transport)
+ [Get More Football Clubs Onboard](https://www.change.org/p/end-gambling-advertising-and-sponsorship-in-football)

### Organisations
* [Gambling With Lives](https://www.gamblingwithlives.org/)
* [GamCare](https://www.gamcare.org.uk/)
* [TheBigStep](https://www.the-bigstep.com/)


## Developers
If you would like to replicate this analysis, clone the repository and see details below.

### Installation
To install the necessary packages, you can use `pip`. Run the following command in your terminal:

```pip install -r requirements.txt```

### Data Sources

Maintained Sources:
* [kaggle_profile](https://www.kaggle.com/nathanhg/datasets) 

Original sources:
  * [premises-licence-register.csv](https://www.gamblingcommission.gov.uk/public-register/premises/download)
  * [postcodes.csv](https://www.doogal.co.uk/ElectoralConstituencies)
  * [ns_sec_2021.csv](https://commonslibrary.parliament.uk/constituency-data-educational-qualifications-2021-census/)
  * [constituencies_deprivation_dashboard.csv](https://commonslibrary.parliament.uk/constituency-data-indices-of-deprivation/)

### Engineering

<table style="border-collapse: collapse; width: 100%;">
  <tr>
    <td style="padding: 10px; vertical-align: top;">
      <strong>Tools:</strong>
      <ul>
        <li>Google Cloud Platform Services</li>
        <li>Deepnote</li>
        <li>Power BI</li>
      </ul>
    </td>
    <td style="padding: 10px; vertical-align: top;">
      <strong>Languages:</strong>
      <ul>
        <li>Python</li>
        <li>SQL</li>
      </ul>
    </td>
  </tr>
</table>

![Data Pipeline](https://github.com/NathanHGayle/gambling_premises_uk/blob/master/diagrams/pipeline_diagram.png)