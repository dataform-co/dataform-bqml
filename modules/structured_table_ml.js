const common = require("./utils");

/**
 * A generic structured table ML pipeline.
 * It incrementally performs an ML operation on rows from the source table
 * and merges to the output table until all rows are processed or runs longer
 * than the specific duration.
 *
 * @param {String} output_table name of the output table
 * @param {String | Array} unique_keys column name(s) for identifying an unique row in the source table
 * @param {String} ml_function the name of the BQML function to call
 * @param {Resolvable} ml_model the remote model to use for the ML operation
 * @param {String | Function} source_query either a query string or a Contextable function to produce the
 *                            query on the source data for the ML operation and it must have the unique key
 *                            columns selected in addition to other fields
 * @param {String} accept_filter a SQL boolean expression for accepting a row to the output table after
 *                 the ML operation
 * @param {Object} ml_configs configurations for the ML operation
 * @param {Number} batch_size number of rows to process in each SQL job. Rows in the object table will be
 *                 processed in batches according to the batch size. Default batch size is 10000
 * @param {Number} batch_duration_secs the number of seconds to pass before breaking the batching loop if it
 *                 hasn't been finished before within this duration. Default value is 22 hours
 */
function table_ml(output_table, unique_keys, ml_function, ml_model, source_query, accept_filter, ml_configs = {}, {
    batch_size = 10000,
    batch_duration_secs = 22 * 60 * 60,
} = {}) {
    let source_func = (source_query instanceof Function) ? source_query : () => source_query;
    let limit_clause = `LIMIT ${batch_size}`;
    let ml_configs_string = Object.entries(ml_configs).map(([k, v]) => `${JSON.stringify(v)} AS ${k}`).join(',');

    unique_keys = (unique_keys instanceof Array ? unique_keys : [unique_keys]);

    // Initialize by creating the output table.
    operate(`init_${output_table}`)
        .queries((ctx) => `CREATE TABLE IF NOT EXISTS ${ctx.resolve(output_table)} AS 
            SELECT * FROM ${ml_function} (
                MODEL ${ctx.ref(ml_model)},
                (SELECT * FROM (${source_func(ctx)}) ${limit_clause}),
                STRUCT (${ml_configs_string})
            ) WHERE ${accept_filter}`);

    // Incrementally update the output table.
    let table = publish(output_table, {
        type: "incremental",
        dependencies: [`init_${output_table}`],
        uniqueKey: unique_keys,
    });

    // Repeatedly find new rows from the source table, performs the ML operation, and merges the result to the output table
    table.preOps((ctx) => `${ctx.when(ctx.incremental(), `
        REPEAT --;`)}`);
    table.query((ctx) => `
        SELECT * FROM ${ml_function} (
            MODEL ${ctx.ref(ml_model)},
            (SELECT S.* FROM (${source_func(ctx)}) AS S
                ${ctx.when(ctx.incremental(), 
                `WHERE NOT EXISTS (SELECT * FROM ${ctx.resolve(output_table)} AS T WHERE ${unique_keys.map((k) => `S.${k} = T.${k}`).join(' AND ')})`)} ${limit_clause}),
            STRUCT (${ml_configs_string})
        ) WHERE ${accept_filter}`);
    table.postOps((ctx) => `${ctx.when(ctx.incremental(), `
        UNTIL (SELECT @@row_count) = 0 OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), @@script.creation_time, SECOND) >= ${batch_duration_secs}
        END REPEAT`, ``)}`);
}

/**
 * Performs the ML.GENERATE_EMBEDDING function on the given source table.
 *
 * @param {String} output_table the name of the table to store the final result
 * @param {String | Array} unique_keys column name(s) for identifying an unique row in the source table
 * @param {Resolvable} ml_model the remote model to use for the ML operation that uses one of the
 *                     `textembedding-gecko*` Vertex AI LLMs as endpoint
 * @param {String | Function} source_query either a query string or a Contextable function to produce the
 *                            query on the source data for the ML operation and it must have the unique key
 *                            columns selected in addition to other fields
 * @param {Object} ml_configs configurations for the ML operation
 * @param {Object} options the configuration object for the {@link table_ml} function
 *
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-embedding}
 */
function generate_embedding(output_table, unique_keys, ml_model, source_query, ml_configs, options) {
    table_ml(output_table, unique_keys, "ML.GENERATE_EMBEDDING", ml_model, source_query,
        common.retryable_error_filter("ml_generate_embedding_status"), ml_configs, options);
}

/**
 * Performs the ML.GENERATE_TEXT function on the given source table.
 *
 * @param {String} output_table the name of the table to store the final result
 * @param {String | Array} unique_keys column name(s) for identifying an unique row in the source table
 * @param {Resolvable} ml_model the remote model to use for the ML operation that uses one
 *                     of the Vertex AI LLM endpoints
 * @param {String | Function} source_query either a query string or a Contextable function to produce the
 *                            query on the source data for the ML operation and it must have the unique key
 *                            columns selected in addition to other fields
 * @param {Object} ml_configs configurations for the ML operation
 * @param {Object} options the configuration object for the {@link table_ml} function
 *
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-text}
 */
function generate_text(output_table, unique_keys, ml_model, source_query, ml_configs, options) {
    table_ml(output_table, unique_keys, "ML.GENERATE_TEXT", ml_model, source_query,
        common.retryable_error_filter("ml_generate_text_status"), ml_configs, options);
}

/**
 * Performs the ML.UNDERSTAND_TEXT function on the given source table.
 *
 * @param {String} output_table the name of the table to store the final result
 * @param {String | Array} unique_keys column name(s) for identifying an unique row in the source table
 * @param {Resolvable} ml_model the remote model with a REMOTE_SERVICE_TYPE of CLOUD_AI_NATURAL_LANGUAGE_V1
 * @param {String | Function} source_query either a query string or a Contextable function to produce the
 *                            query on the source data for the ML operation and it must have the unique key
 *                            columns selected in addition to other fields
 * @param {Object} ml_configs configurations for the ML operation
 * @param {Object} options the configuration object for the {@link table_ml} function
 *
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-understand-text}
 */
function understand_text(output_table, unique_keys, ml_model, source_query, ml_configs, options) {
    table_ml(output_table, unique_keys, "ML.UNDERSTAND_TEXT", ml_model, source_query,
        common.retryable_error_filter("ml_understand_text_status"), ml_configs, options);
}

/**
 * Performs the ML.TRANSLATE function on the given source table.
 *
 * @param {String} output_table the name of the table to store the final result
 * @param {String | Array} unique_keys column name(s) for identifying an unique row in the source table
 * @param {Resolvable} ml_model the remote model with a REMOTE_SERVICE_TYPE of CLOUD_AI_TRANSLATE_V3
 * @param {String | Function} source_query either a query string or a Contextable function to produce the
 *                            query on the source data for the ML operation and it must have the unique key
 *                            columns selected in addition to other fields
 * @param {Object} ml_configs configurations for the ML operation
 * @param {Object} options the configuration object for the {@link table_ml} function
 *
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-translate}
 */
function translate(output_table, unique_keys, ml_model, source_query, ml_configs, options) {
    table_ml(output_table, unique_keys, "ML.TRANSLATE", ml_model, source_query,
        common.retryable_error_filter("ml_translate_status"), ml_configs, options);
}

module.exports = {
    table_ml: table_ml,
    generate_embedding: generate_embedding,
    generate_text: generate_text,
    understand_text: understand_text,
    translate: translate,
}
