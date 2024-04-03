# BigQuery Remote Inference Pipeline
BigQuery supports remote models, such as Vertex AI LLMs, to perform remote inference operations 
on both structured and unstructured data. When using remote inference, the user needs to pay
attention to [quotas and limits](https://cloud.google.com/bigquery/quotas#cloud_ai_service_functions), 
which can result in retryable error in a subset of rows and require reprocessing.

The BQML dataform library assists users to create BQML pipelines that are resilient to 
transient failures by automatic reprocessing and incrementally updating the output table.

## Quick Start Guide
### Installation
Add the bqml package to your package.json file in your Dataform project. 
You can find the most up to date package version on the releases page.

### Usage
The following example shows how to generate text from images using the
Vertex AI multimodel.

```javascript
// Import the module
const bqml = require("bqml");

// Name of the multimodel that has `gemini-pro-vision` as the endpoint
let model = "multi-llm";
// Name of the object table that points to a set of images
let source_table = "product_image";
// Name of the table for storing the result
let output_table = "product_image_description";

// Execute the pipeline
bqml.vision_generate_text(
    source_table, output_table, model, 
    "Describe the image in 20 words", {
        flatten_json_output: true
    }
);
```

## Function Reference
### Function generate_text
#### Signature
```javascript
function generate_text(
    source_table, output_table, unique_keys,
    ml_model, source_query, ml_configs, options)
```
#### Description
Performs the ML.GENERATE_TEXT function on the given source table.

**See**: [https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-text](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-text)

| Param | Type | Description |
| --- | --- | --- |
| source_table | <code>Resolvable</code> | represents the source table |
| output_table | <code>String</code> | the name of the table to store the final result |
| unique_keys | <code>String</code> \| <code>Array</code> | column name(s) for identifying an unique row in the source table |
| ml_model | <code>Resolvable</code> | the remote model to use for the ML operation that uses one of the Vertex AI LLM endpoints |
| source_query | <code>String</code> \| <code>function</code> | either a query string or a Contextable function to produce the query on the source data for the ML operation and it must have the unique key columns selected in addition to other fields |
| ml_configs | <code>Object</code> | configurations for the ML operation |
| options | <code>Object</code> | the configuration object for the [table_ml](#table_ml) function |

---
### Function vision_generate_text
#### Signature
```javascript
function vision_generate_text(
    source_table, output_table, model, prompt, llm_config, options)
```
#### Description
Performs the ML.GENERATE_TEXT function on visual content in the given source table.

**See**: [https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-text#gemini-pro-vision](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-text#gemini-pro-vision)

| Param | Type | Description |
| --- | --- | --- |
| source_table | <code>Resolvable</code> | represents the source object table |
| output_table | <code>String</code> | name of the output table |
| model | <code>Resolvable</code> | name the remote model with the `gemini-pro-vision` endpoint |
| prompt | <code>String</code> | the prompt text for the LLM |
| llm_config | <code>Object</code> | extra configurations to the LLM |
| options | <code>Object</code> | the configuration object for the [obj_table_ml](#obj_table_ml) function |

---
### Function generate_embedding
#### Signature
```javascript
function generate_embedding(
    source_table, output_table, unique_keys,
    ml_model, source_query, ml_configs, options = {})
```
#### Description
Performs the ML.GENERATE_EMBEDDING function on the given source table.

**See**: [https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-embedding](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-embedding)

| Param | Type | Description |
| --- | --- | --- |
| source_table | <code>Resolvable</code> | represents the source table |
| output_table | <code>String</code> | the name of the table to store the final result |
| unique_keys | <code>String</code> \| <code>Array</code> | column name(s) for identifying an unique row in the source table |
| ml_model | <code>Resolvable</code> | the remote model to use for the ML operation that uses one of the `textembedding-gecko*` Vertex AI LLMs as endpoint |
| source_query | <code>String</code> \| <code>function</code> | either a query string or a Contextable function to produce the query on the source data for the ML operation and it must have the unique key columns selected in addition to other fields |
| ml_configs | <code>Object</code> | configurations for the ML operation |
| options | <code>Object</code> | the configuration object for the [table_ml](#table_ml) function |

---
### Function understand_text
#### Signature
```javascript
function understand_text(
    source_table, output_table, unique_keys,
    ml_model, source_query, ml_configs, options)
```
#### Description
Performs the ML.UNDERSTAND_TEXT function on the given source table.

**See**: [https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-understand-text](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-understand-text)

| Param | Type | Description |
| --- | --- | --- |
| source_table | <code>Resolvable</code> | represents the source table |
| output_table | <code>String</code> | the name of the table to store the final result |
| unique_keys | <code>String</code> \| <code>Array</code> | column name(s) for identifying an unique row in the source table |
| ml_model | <code>Resolvable</code> | the remote model with a REMOTE_SERVICE_TYPE of CLOUD_AI_NATURAL_LANGUAGE_V1 |
| source_query | <code>String</code> \| <code>function</code> | either a query string or a Contextable function to produce the query on the source data for the ML operation and it must have the unique key columns selected in addition to other fields |
| ml_configs | <code>Object</code> | configurations for the ML operation |
| options | <code>Object</code> | the configuration object for the [table_ml](#table_ml) function |

---
### Function translate
#### Signature
```javascript
function translate(
    source_table, output_table, unique_keys,
    ml_model, source_query, ml_configs, options)
```
#### Description
Performs the ML.TRANSLATE function on the given source table.

**See**: [https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-translate](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-translate)

| Param | Type | Description |
| --- | --- | --- |
| source_table | <code>Resolvable</code> | represents the source table |
| output_table | <code>String</code> | the name of the table to store the final result |
| unique_keys | <code>String</code> \| <code>Array</code> | column name(s) for identifying an unique row in the source table |
| ml_model | <code>Resolvable</code> | the remote model with a REMOTE_SERVICE_TYPE of CLOUD_AI_TRANSLATE_V3 |
| source_query | <code>String</code> \| <code>function</code> | either a query string or a Contextable function to produce the query on the source data for the ML operation and it must have the unique key columns selected in addition to other fields |
| ml_configs | <code>Object</code> | configurations for the ML operation |
| options | <code>Object</code> | the configuration object for the [table_ml](#table_ml) function |

---
### Function annotate_image
#### Signature
```javascript
function annotate_image(
    source_table, output_table, model, features, options)
```
#### Description
Performs the ML.ANNOTATE_IMAGE function on the given source table.

**See**: [https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-annotate-image](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-annotate-image)

| Param | Type | Description |
| --- | --- | --- |
| source_table | <code>Resolvable</code> | represents the source object table |
| output_table | <code>String</code> | name of the output table |
| model | <code>Resolvable</code> | the remote model with a REMOTE_SERVICE_TYPE of CLOUD_AI_VISION_V1 |
| features | <code>Array</code> | specifies one or more feature names of supported Vision API features |
| options | <code>Object</code> | the configuration object for the [obj_table_ml](#obj_table_ml) function |

---
### Function transcribe
#### Signature
```javascript
function transcribe(
    source_table, output_table, model, recognition_config, options)
```
#### Description
Performs the ML.TRANSCRIBE function on the given source table.

**See**: [https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-transcribe](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-transcribe)

| Param | Type | Description |
| --- | --- | --- |
| source_table | <code>Resolvable</code> | represents the source object table |
| output_table | <code>String</code> | name of the output table |
| model | <code>Resolvable</code> | the remote model with a REMOTE_SERVICE_TYPE of CLOUD_AI_SPEECH_TO_TEXT_V2 |
| recognition_config | <code>Object</code> | the recognition configuration to override the default configuration of the specified recognizer |
| options | <code>Object</code> | the configuration object for the [obj_table_ml](#obj_table_ml) function |

---
### Function process_document
#### Signature
```javascript
function process_document(
    source_table, output_table, model, options)
```
#### Description
Performs the ML.PROCESS_DOCUMENT function on the given source table.

**See**: [https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-process-document](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-process-document)

| Param | Type | Description |
| --- | --- | --- |
| source_table | <code>Resolvable</code> | represents the source object table |
| output_table | <code>String</code> | name of the output table |
| model | <code>Resolvable</code> | the remote model with a REMOTE_SERVICE_TYPE of CLOUD_AI_DOCUMENT_V1 |
| options | <code>Object</code> | the configuration object for the [obj_table_ml](#obj_table_ml) function |

---
<a name="table_ml"></a>
### Function table_ml
#### Signature
```javascript
function table_ml(
    output_table, unique_keys, ml_function, ml_model,
    source_query, accept_filter, ml_configs = {}, {
      batch_size = 10000,
      batch_duration_secs = 22 * 60 * 60
} = {})
```
#### Description
A generic structured table ML pipeline.
It incrementally performs an ML operation on rows from the source table
and merges to the output table until all rows are processed or runs longer
than the specific duration.

| Param | Type | Description |
| --- | --- | --- |
| output_table | <code>String</code> | name of the output table |
| unique_keys | <code>String</code> \| <code>Array</code> | column name(s) for identifying an unique row in the source table |
| ml_function | <code>String</code> | the name of the BQML function to call |
| ml_model | <code>Resolvable</code> | the remote model to use for the ML operation |
| source_query | <code>String</code> \| <code>function</code> | either a query string or a Contextable function to produce the query on the source data for the ML operation and it must have the unique key columns selected in addition to other fields |
| accept_filter | <code>String</code> | a SQL boolean expression for accepting a row to the output table after the ML operation |
| ml_configs | <code>Object</code> | configurations for the ML operation |
| batch_size | <code>Number</code> | number of rows to process in each SQL job. Rows in the object table will be processed in batches according to the batch size. Default batch size is 10000 |
| batch_duration_secs | <code>Number</code> | the number of seconds to pass before breaking the batching loop if it hasn't been finished before within this duration. Default value is 22 hours |

---
<a name="obj_table_ml"></a>
### Function obj_table_ml
#### Signature
```javascript
function obj_table_ml(
    source_table, source, output_table, accept_filter, {
      batch_size = 500,
      unique_key = "uri",
      updated_column = "updated",
      batch_duration_secs = 22 * 60 * 60,
} = {})
```
#### Description
A generic object table ML pipeline.
It incrementally performs an ML operation on new rows from the source table
and merges to the output table until no new row is detected or runs longer
than the specific duration.
A row from the source table is considered as new if the `unique_key` (default to "uri")
of a row is absent in the output table, or if the `updated_column` (default to "updated")
column is newer than the largest value in the output table.

| Param | Type | Description |
| --- | --- | --- |
| source_table | <code>Resolvable</code> | represents the source object table |
| source | <code>String</code> \| <code>function</code> | either a query string or a Contextable function to produce the query on the source data |
| output_table | <code>String</code> | the name of the table to store the final result |
| accept_filter | <code>String</code> | a SQL expression for finding rows that contains retryable error |
| batch_size | <code>Number</code> | number of rows to process in each SQL job. Rows in the object table will be processed in batches according to the batch size. Default batch size is 500 |
| unique_key | <code>String</code> | the primary key in the output table for incremental update. Default value is "uri". |
| updated_column | <code>String</code> | the column that carries the last updated timestamp of an object in the object table. Default value is "updated" |
| batch_duration_secs | <code>Number</code> | the number of seconds to pass before breaking the batching loop if it hasn't been finished before within this duration. Default value is 22 hours |
