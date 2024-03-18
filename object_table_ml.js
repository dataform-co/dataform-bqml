/**
 * Declare the given source a resolvable Dataform data source. 
 */
const declare_resolvable = (source) => {
    if (source.constructor === Object) {
        declare(source);
    } else {
        declare({name: source});
    }
};

/**
 * A generic object table ML pipeline. 
 * It incrementally performs ML operation on rows from the source table and merge to the target table until
 * no new row is detected or ran longer than the specific duration.
 * 
 * @param {Function} source_func a Contextable function to produce the query on the source data
 * @param {String} target_table the name of the table to store the final result
 * @param {String} status_column name of column that carries the ML operation status
 * @param {Number} batch_size number of rows to process in each SQL job. A negative value means unlimited.
 *                 If the batch size is non-negative, the rows will be processed in batches according to the
 *                 batch size, with each batch has a 6 hours of timeout.
 * @param {String} unique_key the primary key in the target table for incremental update, default value is "uri"
 * @param {String} updated_column the column that carries the last updated timestamp of an object in the object table
 * @param {Number} batch_duration_secs if batch size is non-negative, this represents the number of seconds to pass
 *                 before breaking the batching loop if it hasn't been finished before within this duration.
 */
const obj_table_ml = (source_func, target_table, status_column, {
    batch_size,
    unique_key = "uri",
    updated_column = "updated",
    batch_duration_secs = 12 * 60 * 60,
} = {}) => {
    operate(`init_${target_table}`)
    .queries((ctx) => 
        `CREATE TABLE IF NOT EXISTS ${ctx.resolve(target_table)} AS ${source_func(ctx)} WHERE ${status_column} NOT LIKE 'A retryable error occurred:%' 
        ${batch_size >= 0 ? `LIMIT ${batch_size}` : ''}`);

    let table = publish(target_table, {
        type: "incremental",
        dependencies: [`init_${target_table}`],
        uniqueKey: [unique_key]
    });

    if (batch_size >= 0) {
      table.preOps((ctx) => `${ctx.when(ctx.incremental(), `REPEAT --;`, ``)}`);
    }
    table.query(
        (ctx) => `${source_func(ctx)} WHERE ${status_column} NOT LIKE 'A retryable error occurred:%' 
            ${ctx.when(ctx.incremental(), 
                `AND (${unique_key} NOT IN (SELECT ${unique_key} FROM ${ctx.self()}) OR ${updated_column} > (SELECT max(${updated_column}) FROM ${ctx.self()}))`)} 
            ${batch_size >= 0 ? `LIMIT ${batch_size}` : ''}`);
    if (batch_size >= 0) {
        table.postOps((ctx) => `${ctx.when(ctx.incremental(), 
        `UNTIL (SELECT @@row_count) = 0 OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), @@script.creation_time, SECOND) >= ${batch_duration_secs}
         END REPEAT`, ``)}`
        );
    }
};

const annotate_image = (source_table, target_table, model, features, options = {}) => {
    let feature_names = features.map((f) => `'${f}'`).join(", ");
    declare_resolvable(source_table);
    declare_resolvable(model);
    // Vision API has 1800 per minute quota, 6 hrs of job is ~650K of calls, hence set the batch size to 500K
    obj_table_ml((ctx) => `SELECT * FROM ML.ANNOTATE_IMAGE(
        MODEL ${ctx.resolve(model)},
        TABLE ${ctx.resolve(source_table)},
        STRUCT([${feature_names}] AS vision_features))`, 
        target_table, "ml_annotate_image_status", {...{batch_size : 500000}, ...options});
};

const transcribe = (source_table, target_table, model, recognition_config, options = {}) => {
    let config = JSON.stringify(recognition_config);
    declare_resolvable(source_table);
    declare_resolvable(model);
    // Speech to text API has 900 per minute quota, 6 hrs of job is ~320K of calls, hence set the batch size to 250K
    obj_table_ml((ctx) => `SELECT * FROM ML.TRANSCRIBE(
        MODEL ${ctx.resolve(model)},
        TABLE ${ctx.resolve(source_table)},
        recognition_config => ( JSON '${config}'))`, 
        target_table, "ml_transcribe_status", {...{batch_size : 250000}, ...options});
};

const process_document = (source_table, target_table, model, options = {}) => {
    declare_resolvable(source_table);
    declare_resolvable(model);
    // Document API has 1800 per minute quota, 6 hrs of job is ~650K of calls, hence set the batch size to 500K
    obj_table_ml((ctx) => `SELECT * FROM ML.PROCESS_DOCUMENT(
        MODEL ${ctx.resolve(model)},
        TABLE ${ctx.resolve(source_table)})`, 
        target_table, "ml_process_document_status", {...{batch_size : 500000}, ...options});
};

module.exports = {
    annotate_image: annotate_image,
    transcribe: transcribe,
    process_document: process_document,
    obj_table_ml: obj_table_ml,
};
