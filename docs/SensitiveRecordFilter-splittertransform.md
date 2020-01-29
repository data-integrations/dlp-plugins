# Cloud Data Loss Prevention (DLP) PII Filter


Additional Charges
-----------
This plugin uses the Data Loss prevention APIs which charge the user depending 
on the volume of data analysed. More details on the exact 
costs can be found [here](https://cloud.google.com/dlp/pricing#content-pricing). 

Permissions
-----------
In order for this plugin to function, it requires permission to access the Data Loss Prevention APIs. These permissions
granted through the service account that is provided in the plugin configuration. If the service account path is set to 
`auto-detect` then it will use a service account with the name `service-<project-number>@gcp-sa-datafusion.iam.gserviceaccount.com`.

The `DLP Administrator` role must be granted to the service account to allow this plugin to access the DLP APIs.

Description
-----------
This plugin separates sensitive records from the input stream. A record is deemed sensitive if it matches a user-defined template. More info on creating templates can be found [here](https://cloud.google.com/dlp/docs/creating-templates-inspect#about_templates). 

The matching can be applied to the entire record or a particular field (recommended if the entire record is large, DLP supports a maximum of 0.5MB per record)

There are three options for error handling in this plugin:
 * **Stop pipeline**: Stops the pipeline as soon as an error is encountered
 * **Skip record**: The record that caused the error will be skipped and no error will be reported
 * **Send to error**: Send errors to the error port and continue running the pipeline

Metrics
-----------
This plugin records three metrics:
* `dlp.requests.count`: Total number of requests sent to Data Loss Prevention API
* `dlp.requests.success`: Number of requests that were successfully processed by Data Loss Prevention API
* `dlp.requests.fail`: Number of requests that failed