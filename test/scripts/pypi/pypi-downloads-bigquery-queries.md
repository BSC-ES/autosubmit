## Number of downloads in a year

This is a common request for EU proposals. You can follow the instructions
provided by PyPI to access Google BigQuery, and query this information.

<https://packaging.python.org/en/latest/guides/analyzing-pypi-package-downloads/>

Here's an example to fetch the number of downloads in 2025.

```bigquery
SELECT COUNT(*) AS num_downloads
FROM `bigquery-public-data.pypi.file_downloads`
WHERE file.project = 'autosubmit'
  AND DATE(timestamp)
    BETWEEN PARSE_DATE('%Y%m%d', '20250101')
    AND PARSE_DATE('%Y%m%d', '20251231')
```
