const common = require("./common.js");

/**
 * A generic object table ML pipeline. 
 * It incrementally performs an ML operation on new rows from the source table 
 * and merges to the target table until no new row is detected or runs longer 
 * than the specific duration.
 * A row from the source table is considered as new if the `unique_key` (default to "uri") 
 * of a row is absent in the target table, or if the `updated_column` (default to "updated") 
 * column is newer than the largest value in the target table.
 * 
 * @param {Resolvable} source_table represents the source object table
 * @param {String | Function} source either a query string or a Contextable function to produce the query on the source data
 * @param {String} target_table the name of the table to store the final result
 * @param {String} accept_filter a SQL expression for finding rows that contains retryable error
 * @param {Number} batch_size number of rows to process in each SQL job. Rows in the object table will be 
 *                 processed in batches according to the batch size. Default batch size is 500
 * @param {String} unique_key the primary key in the target table for incremental update. Default value is "uri".
 * @param {String} updated_column the column that carries the last updated timestamp of an object in the object 
 *                 table. Default value is "updated"
 * @param {Number} batch_duration_secs the number of seconds to pass before breaking the batching loop if it 
 *                 hasn't been finished before within this duration. Default value is 22 hours
 */
const obj_table_ml = (source_table, source, target_table, accept_filter, {
    batch_size = 500,
    unique_key = "uri",
    updated_column = "updated",
    batch_duration_secs = 22 * 60 * 60,
} = {}) => {
    let source_func = (source instanceof Function) ? source : () => source;
    let limit_clause = `LIMIT ${batch_size}`;

    // Initialize by creating the target table with a small limit to avoid timeout
    operate(`init_${target_table}`)
        .queries((ctx) =>
            `CREATE TABLE IF NOT EXISTS ${ctx.resolve(target_table)} AS ${source_func(ctx)} WHERE ${accept_filter} LIMIT 10`);

    // Incrementally update the target table.
    let table = publish(target_table, {
        type: "incremental",
        dependencies: [`init_${target_table}`],
        uniqueKey: [unique_key]
    });

    // Repeatedly finding a new set of uri candidates, performs the ML operation, and merges the result to the target table
    table.preOps((ctx) => `${ctx.when(ctx.incremental(), `
        DECLARE candidates ARRAY<STRING>;
        REPEAT
            SET candidates = ARRAY(
                SELECT ${unique_key} FROM ${ctx.resolve(source_table)} AS S 
                WHERE NOT EXISTS (SELECT * FROM ${ctx.resolve(target_table)} AS T WHERE S.${unique_key} = T.${unique_key})
                    OR ${updated_column} > (SELECT max(${updated_column}) FROM ${ctx.resolve(target_table)}) ${limit_clause})`,
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
};

/**
 * Performs the ML.ANNOTATE_IMAGE function on the given source table.
 * 
 * @param {Resolvable} source_table represents the source object table
 * @param {String} target_table name of the target table
 * @param {Resolvable} model the remote model with a REMOTE_SERVICE_TYPE of CLOUD_AI_VISION_V1
 * @param {Array} features specifies one or more feature names of supported Vision API features
 * @param {Object} options the configuration object for the {@link obj_table_ml} function
 * 
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-annotate-image}
 */
const annotate_image = (source_table, target_table, model, features, options) => {
    let feature_names = features.map((f) => `'${f}'`).join(", ");
    common.declare_resolvable(source_table);
    common.declare_resolvable(model);
    obj_table_ml(source_table, (ctx) => `SELECT * FROM ML.ANNOTATE_IMAGE(
        MODEL ${ctx.resolve(model)},
        TABLE ${ctx.resolve(source_table)},
        STRUCT([${feature_names}] AS vision_features))`,
        target_table, common.retryable_error_filter("ml_annotate_image_status"), options);
};

/**
 * Performs the ML.TRANSCRIBE function on the given source table.
 * 
 * @param {Resolvable} source_table represents the source object table
 * @param {String} target_table name of the target table
 * @param {Resolvable} model the remote model with a REMOTE_SERVICE_TYPE of CLOUD_AI_SPEECH_TO_TEXT_V2
 * @param {Object} recognition_config the recognition configuration to override the default configuration 
 *                 of the specified recognizer
 * @param {Object} options the configuration object for the {@link obj_table_ml} function
 * 
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-transcribe}
 */
const transcribe = (source_table, target_table, model, recognition_config, options) => {
    let config = JSON.stringify(recognition_config);
    common.declare_resolvable(source_table);
    common.declare_resolvable(model);
    obj_table_ml(source_table, (ctx) => `SELECT * FROM ML.TRANSCRIBE(
        MODEL ${ctx.resolve(model)},
        TABLE ${ctx.resolve(source_table)},
        recognition_config => ( JSON '${config}'))`,
        target_table, common.retryable_error_filter("ml_transcribe_status"), options);
};

/**
 * Performs the ML.PROCESS_DOCUMENT function on the given source table.
 * 
 * @param {Resolvable} source_table represents the source object table
 * @param {String} target_table name of the target table
 * @param {Resolvable} model the remote model with a REMOTE_SERVICE_TYPE of CLOUD_AI_DOCUMENT_V1
 * @param {Object} options the configuration object for the {@link obj_table_ml} function
 * 
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-process-document
 */
const process_document = (source_table, target_table, model, options) => {
    common.declare_resolvable(source_table);
    common.declare_resolvable(model);
    obj_table_ml(source_table, (ctx) => `SELECT * FROM ML.PROCESS_DOCUMENT(
        MODEL ${ctx.resolve(model)},
        TABLE ${ctx.resolve(source_table)})`,
        target_table, common.retryable_error_filter("ml_process_document_status"), options);
};

/**
 * Performs the ML.GENERATE_TEXT function on visual content in the given source table.
 * 
 * @param {Resolvable} source_table represents the source object table
 * @param {String} target_table name of the target table
 * @param {Resolvable} model name the remote model with the `gemini-pro-vision` endpoint
 * @param {String} prompt the prompt text for the LLM
 * @param {Object} llm_config extra configurations to the LLM
 * @param {Object} options the configuration object for the {@link obj_table_ml} function
 * 
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-text#gemini-pro-vision
 */
const vision_generate_text = (source_table, target_table, model, prompt, llm_config, options) => {
    let config = {
        prompt: prompt,
        ...llm_config
    };
    common.declare_resolvable(source_table);
    common.declare_resolvable(model);
    obj_table_ml(source_table, (ctx) => `SELECT * FROM ML.GENERATE_TEXT(
        MODEL ${ctx.resolve(model)},
        TABLE ${ctx.resolve(source_table)},
        STRUCT(
            ${Object.entries(config).map(([k, v]) => `${JSON.stringify(v)} AS ${k}`).join(",")}
        ))`,
        target_table, common.retryable_error_filter("ml_generate_text_status"), options);
}

module.exports = {
    annotate_image: annotate_image,
    transcribe: transcribe,
    process_document: process_document,
    vision_generate_text: vision_generate_text,
    obj_table_ml: obj_table_ml,
};
