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
 * It incrementally performs ML operation on rows from the source table and merge to the target table.
 * 
 * @param {Function} source_func a Contextable function to produce the query on the source data
 * @param {String} target_table the name of the table to store the final result
 * @param {status_column} 
 */
const obj_table_ml = (source_func, target_table, status_column, {
    unique_key = "uri",
    updated_column = "updated",
    batch_size = 10000,
} = {}) => {
    publish(target_table, {
        type: "incremental",
        uniqueKey: [unique_key]
    }).query(
        (ctx) => `${source_func(ctx)} WHERE ${status_column} NOT LIKE 'A retryable error occurred:%' 
            ${ctx.when(ctx.incremental(), 
                `AND (${unique_key} NOT IN (SELECT ${unique_key} FROM ${ctx.resolve(target_table)}) OR ${updated_column} > (SELECT max(${updated_column}) FROM ${ctx.resolve(target_table)}))`)} 
            LIMIT ${batch_size}`);
};

const annotate_image = (source_table, target_table, model, features, options = {}) => {
    let feature_names = features.map((f) => `'${f}'`).join(", ");
    declare_resolvable(source_table);
    declare_resolvable(model);
    obj_table_ml((ctx) => `SELECT * FROM ML.ANNOTATE_IMAGE(
        MODEL ${ctx.resolve(model)},
        TABLE ${ctx.resolve(source_table)},
        STRUCT([${feature_names}] AS vision_features))`, 
        target_table, "ml_annotate_image_status", options);
};

const transcribe = (source_table, target_table, model, recognition_config, options = {}) => {
    let config = JSON.stringify(recognition_config);
    declare_resolvable(source_table);
    declare_resolvable(model);
    obj_table_ml((ctx) => `SELECT * FROM ML.TRANSCRIBE(
        MODEL ${ctx.resolve(model)},
        TABLE ${ctx.resolve(source_table)},
        recognition_config => ( JSON '${config}'))`, 
        target_table, "ml_transcribe_status", options);
};

const process_document = (source_table, target_table, model, options = {}) => {
    declare_resolvable(source_table);
    declare_resolvable(model);
    obj_table_ml((ctx) => `SELECT * FROM ML.PROCESS_DOCUMENT(
        MODEL ${ctx.resolve(model)},
        TABLE ${ctx.resolve(source_table)})`, 
        target_table, "ml_process_document_status", options);
};

module.exports = {
    annotate_image: annotate_image,
    transcribe: transcribe,
    process_document: process_document,
    obj_table_ml: obj_table_ml,
};
