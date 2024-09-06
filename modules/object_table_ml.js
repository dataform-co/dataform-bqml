const common = require("./utils");

/**
 * A generic object table ML pipeline. 
 * It incrementally performs an ML operation on new rows from the source table 
 * and merges to the output table until no new row is detected or runs longer 
 * than the specific duration.
 * A row from the source table is considered as new if the `unique_key` (default to "uri") 
 * of a row is absent in the output table, or if the `updated_column` (default to "updated") 
 * column is newer than the largest value in the output table.
 * 
 * @param {Resolvable} source_table represents the source object table
 * @param {String | Function} source either a query string or a Contextable function to produce the query on the source data
 * @param {String} output_table the name of the table to store the final result
 * @param {String} accept_filter a SQL expression for finding rows that contains retryable error
 * @param {Number} batch_size number of rows to process in each SQL job. Rows in the object table will be 
 *                 processed in batches according to the batch size. Default batch size is 500
 * @param {String} unique_key the primary key in the output table for incremental update. Default value is "uri".
 * @param {String} updated_column the column that carries the last updated timestamp of an object in the object 
 *                 table. Default value is "updated"
 * @param {Number} batch_duration_secs the number of seconds to pass before breaking the batching loop if it 
 *                 hasn't been finished before within this duration. Default value is 22 hours
 */
function obj_table_ml(source_table, source, output_table, accept_filter, {
    batch_size = 500,
    unique_key = "uri",
    updated_column = "updated",
    batch_duration_secs = 22 * 60 * 60,
} = {}) {
    let source_func = (source instanceof Function) ? source : () => source;
    let limit_clause = `LIMIT ${batch_size}`;

    // Initialize by creating the output table with a small limit to avoid timeout
    operate(`init_${output_table}`)
        .queries((ctx) =>
            `CREATE TABLE IF NOT EXISTS ${ctx.resolve(output_table)} AS ${source_func(ctx)} WHERE ${accept_filter} LIMIT 10`);

    // Incrementally update the output table.
    let table = publish(output_table, {
        type: "incremental",
        dependencies: [`init_${output_table}`],
        uniqueKey: [unique_key]
    });

    // Repeatedly finding a new set of uri candidates, performs the ML operation, and merges the result to the output table
    table.preOps((ctx) => `${ctx.when(ctx.incremental(), `
        DECLARE candidates ARRAY<STRING>;
        REPEAT
            SET candidates = ARRAY(
                SELECT ${unique_key} FROM ${ctx.ref(source_table)} AS S 
                WHERE NOT EXISTS (SELECT * FROM ${ctx.resolve(output_table)} AS T WHERE S.${unique_key} = T.${unique_key})
                    OR ${updated_column} > (SELECT max(${updated_column}) FROM ${ctx.resolve(output_table)}) ${limit_clause})`,
        ``)}`);
    table.query((ctx) => `
        ${source_func(ctx)} WHERE ${ctx.when(ctx.incremental(), 
            `${unique_key} IN UNNEST(candidates) AND ${accept_filter}`, 
            // The non-incremental part shouldn't be used since the table is already created in the init operation above.
            // Nevertheless, the accept filter and limit is set if such occassion does occur.
            `${accept_filter} ${limit_clause}`)}`);
    table.postOps((ctx) => `${ctx.when(ctx.incremental(), `
            UNTIL (SELECT @@row_count) = 0 OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), @@script.creation_time, SECOND) >= ${batch_duration_secs} 
        END REPEAT`)}`);
}

/**
 * Performs the ML.ANNOTATE_IMAGE function on the given source table.
 * 
 * @param {Resolvable} source_table represents the source object table
 * @param {String} output_table name of the output table
 * @param {Resolvable} model the remote model with a REMOTE_SERVICE_TYPE of CLOUD_AI_VISION_V1
 * @param {Array} features specifies one or more feature names of supported Vision API features
 * @param {Object} options the configuration object for the {@link obj_table_ml} function
 * 
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-annotate-image}
 */
function annotate_image(source_table, output_table, model, features, options) {
    let feature_names = features.map((f) => `'${f}'`).join(", ");

    obj_table_ml(source_table, (ctx) => `SELECT * FROM ML.ANNOTATE_IMAGE(
        MODEL ${ctx.ref(model)},
        TABLE ${ctx.ref(source_table)},
        STRUCT([${feature_names}] AS vision_features))`,
        output_table, common.retryable_error_filter("ml_annotate_image_status"), options);
}

/**
 * Performs the ML.TRANSCRIBE function on the given source table.
 * 
 * @param {Resolvable} source_table represents the source object table
 * @param {String} output_table name of the output table
 * @param {Resolvable} model the remote model with a REMOTE_SERVICE_TYPE of CLOUD_AI_SPEECH_TO_TEXT_V2
 * @param {Object} recognition_config the recognition configuration to override the default configuration 
 *                 of the specified recognizer
 * @param {Object} options the configuration object for the {@link obj_table_ml} function
 * 
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-transcribe}
 */
function transcribe(source_table, output_table, model, recognition_config, options) {
    let config = JSON.stringify(recognition_config);

    obj_table_ml(source_table, (ctx) => `SELECT * FROM ML.TRANSCRIBE(
        MODEL ${ctx.ref(model)},
        TABLE ${ctx.ref(source_table)},
        recognition_config => ( JSON '${config}'))`,
        output_table, common.retryable_error_filter("ml_transcribe_status"), options);
}

/**
 * Performs the ML.PROCESS_DOCUMENT function on the given source table.
 * 
 * @param {Resolvable} source_table represents the source object table
 * @param {String} output_table name of the output table
 * @param {Resolvable} model the remote model with a REMOTE_SERVICE_TYPE of CLOUD_AI_DOCUMENT_V1
 * @param {Object} options the configuration object for the {@link obj_table_ml} function
 * 
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-process-document
 */
function process_document(source_table, output_table, model, options) {
    obj_table_ml(source_table, (ctx) => `SELECT * FROM ML.PROCESS_DOCUMENT(
        MODEL ${ctx.ref(model)},
        TABLE ${ctx.ref(source_table)})`,
        output_table, common.retryable_error_filter("ml_process_document_status"), options);
}

/**
 * Performs the ML.GENERATE_TEXT function on visual content in the given source table.
 * 
 * @param {Resolvable} source_table represents the source object table
 * @param {String} output_table name of the output table
 * @param {Resolvable} model name the remote model with the `gemini-pro-vision` endpoint
 * @param {String} prompt the prompt text for the LLM
 * @param {Object} llm_config extra configurations to the LLM
 * @param {Object} options the configuration object for the {@link obj_table_ml} function
 * 
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-text#gemini-pro-vision
 */
function vision_generate_text(source_table, output_table, model, prompt, llm_config, options) {
    let config = {
        prompt: prompt,
        ...llm_config
    };
    obj_table_ml(source_table, (ctx) => `SELECT * FROM ML.GENERATE_TEXT(
        MODEL ${ctx.ref(model)},
        TABLE ${ctx.ref(source_table)},
        STRUCT(
            ${Object.entries(config).map(([k, v]) => `${JSON.stringify(v)} AS ${k}`).join(",")}
        ))`,
        output_table, common.retryable_error_filter("ml_generate_text_status"), options);
}

module.exports = {
    annotate_image: annotate_image,
    transcribe: transcribe,
    process_document: process_document,
    vision_generate_text: vision_generate_text,
    obj_table_ml: obj_table_ml,
};
