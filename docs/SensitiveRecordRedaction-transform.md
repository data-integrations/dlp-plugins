# Cloud Data Loss Prevention (DLP) Redact

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
This plugin transforms sensitive records from the input stream. A record is 
deemed sensitive if it matches some pre-defined DLP filters or a custom user-defined 
template. See the *DLP Filter Mapping* section for more details on the supported 
pre-defined filters. More information about custom templates can be found [here](https://cloud.google.com/dlp/docs/creating-templates-inspect#about_templates).

This plugin currently supports the 5 most commonly used DLP 
transformations:

* **[Date Shift](https://cloud.google.com/dlp/docs/transformations-reference#date-shift)**: Apply a random shift to a date/timestamp value (supported types: `date`, `timestamp`)
* **[Masking](https://cloud.google.com/dlp/docs/transformations-reference#masking)**: Mask sensitive text by replacing characters with the Masking Character (supported types: `string`)
* **[One-way Hash](https://cloud.google.com/dlp/docs/transformations-reference#crypto-hashing)**: Apply a one-way cryptographic hash function to the data (supported types: `all`)
* **[Redact](https://cloud.google.com/dlp/docs/transformations-reference#redaction)**: Remove sensitive text from the record (supported types: `string`)
* **[Replace with value](https://cloud.google.com/dlp/docs/reference/rest/v2/organizations.deidentifyTemplates#DeidentifyTemplate.ReplaceValueConfig)**: Replace sensitive text with a new value (supported types: `string`)
* **[Format Preserving Encryption](https://cloud.google.com/dlp/docs/reference/rest/v2/organizations.deidentifyTemplates#cryptoreplaceffxfpeconfig)**: Replaces sensitive text with an format-preserving encrypted value. The value can be decrypted using the Decrypt Plugin (supported types: `string`)  

DLP Filter Mapping
-----------
This plugin supports most pre-defined DLP filters, they are grouped into boarder
categories for ease of use. The contents of each group are as follows:

* **Demographic**: PERSON_NAME, AGE, DATE_OF_BIRTH, PHONE_NUMBER, ETHNIC_GROUP
* **Location**: LOCATION, MAC_ADDRESS, MAC_ADDRESS_LOCAL
* **Tax IDs**: AUSTRALIA_TAX_FILE_NUMBER, DENMARK_CPR_NUMBER, NORWAY_NI_NUMBER, PORTUGAL_CDC_NUMBER, US_ADOPTION_TAXPAYER_IDENTIFICATION_NUMBER, US_EMPLOYER_IDENTIFICATION_NUMBER,US_PREPARER_TAXPAYER_IDENTIFICATION_NUMBER
* **Credit Card Numbers**: CREDIT_CARD_NUMBER 
* **Passport Numbers**: NETHERLANDS_PASSPORT
* **Health IDs**: US_HEALTHCARE_NPI, CANADA_OHIP
* **National IDs**: CHINA_RESIDENT_ID_NUMBER, DENMARK_CPR_NUMBER, FRANCE_CNI, FRANCE_NIR, FINLAND_NATIONAL_ID_NUMBER, JAPAN_INDIVIDUAL_NUMBER, NORWAY_NI_NUMBER, PARAGUAY_CIC_NUMBER, POLAND_PESEL_NUMBER, POLAND_NATIONAL_ID_NUMBER, PORTUGAL_CDC_NUMBER, SPAIN_NIE_NUMBER, SPAIN_NIF_NUMBER, SWEDEN_NATIONAL_ID_NUMBER, US_SOCIAL_SECURITY_NUMBER, URUGUAY_CDI_NUMBER, VENEZUELA_CDI_NUMBER
* **Driver License IDs**: SPAIN_DRIVERS_LICENSE_NUMBER, US_DRIVERS_LICENSE_NUMBER
<!-- * **Insurance**:  CANADA_SOCIAL_INSURANCE_NUMBER, UK_NATIONAL_INSURANCE_NUMBER -->

Metrics
-----------
This plugin records three metrics:
* `dlp.requests.count`: Total number of requests sent to Data Loss Prevention API
* `dlp.requests.success`: Number of requests that were successfully processed by Data Loss Prevention API
* `dlp.requests.fail`: Number of requests that failed

Custom Template Path
-----------
The option to use a custom template path which is located in a different project other than the one specified in Project Id.