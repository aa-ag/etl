## ETL

Experimenting with ETLs using Python; starting with an ETL process to:

- retreive data using `requests`
- manipulate date using `Pandas`
- write to a database using `BigQuery`

## data

Will be using Citi's New York City's [bikers data feed](https://www.citibikenyc.com/system-data)

Endpoints listed here: http://gbfs.citibikenyc.com/gbfs/gbfs.json

Specifications listed here: https://github.com/NABSA/gbfs/blob/master/gbfs.md#system_informationjson

### env

- `conda create etl`
- `conda activate etl`
- `conda install pandas`
- `pip install pandas-gbq -U`
- `pip install --upgrade google-cloud-bigquery`

"To use the BigQuery API, you must first authenticate to verify your client's identity." --> [documentation](https://cloud.google.com/bigquery/docs/authentication/)