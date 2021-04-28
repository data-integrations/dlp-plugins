# Cloud Data Loss Prevention (DLP) Decrypt

Additional Charges
-----------
This plugin uses Google's Data Loss Prevention APIs which charge the user depending 
on the volume of data **analyzed** (not transformed). More details on the exact 
costs can be found [here](https://cloud.google.com/dlp/pricing#content-pricing). 

Permissions
-----------
In order for this plugin to function, it requires permissions to access the Data Loss Prevention APIs. These permissions
granted through the service account that is provided in the plugin configuration. If the service account path is set to 
`auto-detect` then it will use a service account with the name `service-<project-number>@gcp-sa-datafusion.iam.gserviceaccount.com`.

The `DLP Administrator` role must be granted to the service account to allow this plugin to access the DLP APIs.

Description
-----------
This plugin decrypts sensitive data that was encrypted by DLP using a reversible encryption transform, such as `Format 
Preserving Encryption`. The plugin works by reversing the encryption specified in the config. Therefore, you must provide 
the same configuration properties that were used to encrypt the data. In other words, the configuration in this plugin 
and the DLP Redaction plugin must be identical for the decrypt to function correctly.


Metrics
-----------
This plugin records three metrics:
* `dlp.requests.count`: Total number of requests sent to Data Loss Prevention API
* `dlp.requests.success`: Number of requests that were successfully processed by Data Loss Prevention API
* `dlp.requests.fail`: Number of requests that failed

Custom Template Path
-----------
The option to use a custom template path which is located in a different project other than the one specified in Project Id.