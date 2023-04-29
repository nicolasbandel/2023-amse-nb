# Project Plan

## Summary

<!-- Describe your data science project in max. 5 sentences. -->
This project analyzes the shock data of cargo trains, run buy the DB Cargo AG, in relation to the closest DB Cargo AG station.
The goal is to determine if there are certain location with a high number of shock data. Furthermore, it should be differentiated if these shocks appear 
while the train is moving or if the train is stationary (probably loading or unloading).

## Rationale

<!-- Outline the impact of the analysis, e.g. which pains it solves. -->
The analysis helps DB Cargo AG to determine the areas where the most shocks appear. With that knowledge the DB Cargo AG can have a detailed investigation in the occurrence of the shocks with the goal to reduce them.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Impact data of freight wagons
* Metadata URL: https://mobilithek.info/offers/573487566471229440
* Data URL: https://mobilithek.info/mdp-api/files/aux/573487566471229440/ShockData.csv
* Data Type: CSV

This data source contains the sensor data of the cargo trains. The sensors save data at a given frequency or due to an event.

### Datasource2: List of freight transport locations
* Metadata URL: https://data.deutschebahn.com/dataset/betriebsstellen-gueterverkehr.html
* Data URL: https://download-data.deutschebahn.com/static/datasets/betriebsstellen_cargo/GEO_Bahnstellen_EXPORT.csv
* Data Type: CSV

This data source contains the locations of the DB Cargo AG. The data sample contain information regarding name, country and geographic location.

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Automated data pipeline [#1][i1]
2. Automated tests [#2][i2]
3. Continuous integration [#3][i3]
4. Project deployment on GitHub [#4][i4]
5. Filter relevant shock events [#5][i5]
6. Group shock events by location [#6][i6]
7. Distinguish shock events by speed [#7][i7]
8. Link shock event groups and locations [#8][i8]
9. Display locations [#9][i9]

[i1]: https://github.com/nicolasbandel/2023-amse-nb/issues/1
[i2]: https://github.com/nicolasbandel/2023-amse-nb/issues/2
[i3]: https://github.com/nicolasbandel/2023-amse-nb/issues/3
[i4]: https://github.com/nicolasbandel/2023-amse-nb/issues/4
[i5]: https://github.com/nicolasbandel/2023-amse-nb/issues/5
[i6]: https://github.com/nicolasbandel/2023-amse-nb/issues/6
[i7]: https://github.com/nicolasbandel/2023-amse-nb/issues/7
[i8]: https://github.com/nicolasbandel/2023-amse-nb/issues/8
[i9]: https://github.com/nicolasbandel/2023-amse-nb/issues/9
