const common = require("./common.js");

/**
 * A generic object table ML pipeline. 
 * It incrementally performs ML operation on rows from the source table and merge to the target table until
 * no new row is detected or ran longer than the specific duration.
 * 
 * @param {String} name of the source object table
 * @param {Function | String} source either a query string or a Contextable function to produce the query on the source data
 * @param {String} target_table the name of the table to store the final result
 * @param {String} accept_filter a SQL expression for finding rows that contains retryable error
 * @param {Number} batch_size number of rows to process in each SQL job.
 *                 If the batch size is given, the rows will be processed in batches according to the
 *                 batch size, with each batch has a 6 hours of timeout. Default batch size is 10000.
 * @param {String} unique_key the primary key in the target table for incremental update, default value is "uri"
 * @param {String} updated_column the column that carries the last updated timestamp of an object in the object table
 * @param {Number} batch_duration_secs if batch size is non-negative, this represents the number of seconds to pass
 *                 before breaking the batching loop if it hasn't been finished before within this duration.
 */
const obj_table_ml = (source_table, source, target_table, accept_filter, {
    batch_size = 500,
    unique_key = "uri",
    updated_column = "updated",
    batch_duration_secs = 22 * 60 * 60,
} = {}) => {
    let source_func = (source instanceof Function) ? source : () => source;
    let limit_clause = batch_size >= 0 ? `LIMIT ${batch_size}` : '';

    // Initialize by creating the target table with a small limit to avoid timeout if it doesn't exist
    operate(`init_${target_table}`)
    .queries((ctx) => 
        `CREATE TABLE IF NOT EXISTS ${ctx.resolve(target_table)} AS ${source_func(ctx)} WHERE ${accept_filter} LIMIT 10`);

    // Incrementally update the target table.
    let table = publish(target_table, {
        type: "incremental",
        dependencies: [`init_${target_table}`],
        uniqueKey: [unique_key]
    });

    let where_clause = accept_filter;

    if (batch_size >= 0) {
      table.preOps((ctx) => `
        ${ctx.when(ctx.incremental(), 
            `DECLARE candidates ARRAY<STRING>;
             REPEAT
                SET candidates = ARRAY(SELECT ${unique_key} FROM ${ctx.resolve(source_table)} AS S
                    WHERE NOT EXISTS (SELECT * FROM ${ctx.resolve(target_table)} AS T WHERE S.${unique_key} = T.${unique_key})
                        OR ${updated_column} > (SELECT max(${updated_column}) FROM ${ctx.resolve(target_table)}) ${limit_clause})`,
            ``)}`);
      where_clause = `${unique_key} IN UNNEST(candidates) AND ${accept_filter}`;
    }
    table.query(
        (ctx) => `${source_func(ctx)} WHERE
            ${ctx.when(ctx.incremental(), 
            `${where_clause}`, 
            // The non-incremental part shouldn't be used since the table is already created in the init operation above.
            // Nevertheless, the accept filter and limit is set if such occassion does occur.
            `${accept_filter} ${limit_clause}`)}`);
    if (batch_size >= 0) {
        table.postOps((ctx) => `${ctx.when(ctx.incremental(), 
        `UNTIL (SELECT @@row_count) = 0 OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), @@script.creation_time, SECOND) >= ${batch_duration_secs}
         END REPEAT`, ``)}`
        );
    }
};

const annotate_image = (source_table, target_table, model, features, options = {}) => {
    let feature_names = features.map((f) => `'${f}'`).join(", ");
    common.declare_resolvable(source_table);
    common.declare_resolvable(model);
    obj_table_ml(source_table, (ctx) => `SELECT * FROM ML.ANNOTATE_IMAGE(
        MODEL ${ctx.resolve(model)},
        TABLE ${ctx.resolve(source_table)},
        STRUCT([${feature_names}] AS vision_features))`, 
        target_table, common.retryable_error_filter("ml_annotate_image_status"), options);
};

const transcribe = (source_table, target_table, model, recognition_config, options = {}) => {
    let config = JSON.stringify(recognition_config);
    common.declare_resolvable(source_table);
    common.declare_resolvable(model);
    obj_table_ml(source_table, (ctx) => `SELECT * FROM ML.TRANSCRIBE(
        MODEL ${ctx.resolve(model)},
        TABLE ${ctx.resolve(source_table)},
        recognition_config => ( JSON '${config}'))`, 
        target_table, common.retryable_error_filter("ml_transcribe_status"), options);
};

const process_document = (source_table, target_table, model, options = {}) => {
    common.declare_resolvable(source_table);
    common.declare_resolvable(model);
    obj_table_ml(source_table, (ctx) => `SELECT * FROM ML.PROCESS_DOCUMENT(
        MODEL ${ctx.resolve(model)},
        TABLE ${ctx.resolve(source_table)})`, 
        target_table, common.retryable_error_filter("ml_process_document_status"), options);
};

const vision_generate_text = (source_table, target_table, model, prompt, call_config = {}, options = {}) => {
    let config = {prompt: prompt, ...call_config};
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
