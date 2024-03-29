const common = require("./common.js");

const table_ml = (target_table, unique_keys, ml_function, ml_model, source_query, accept_filter, ml_configs = {}, {
    batch_size = 10000,
    batch_duration_secs = 22 * 60 * 60
} = {}) => {
    let source_func = (source_query instanceof Function) ? source_query : () => source_query;
    let limit_clause = batch_size >= 0 ? `LIMIT ${batch_size}` : '';
    let ml_configs_string = Object.entries(ml_configs).map(([k, v]) => `${JSON.stringify(v)} AS ${k}`).join(',');

    unique_keys = (unique_keys instanceof Array ? unique_keys : [unique_keys]);

    // Initialize by creating the target table.
    operate(`init_${target_table}`)
        .queries((ctx) => `CREATE TABLE IF NOT EXISTS ${ctx.resolve(target_table)} AS 
            SELECT * FROM ${ml_function} (
                MODEL ${ctx.resolve(ml_model)},
                (SELECT * FROM (${source_func(ctx)}) ${limit_clause}),
                STRUCT (${ml_configs_string})
            ) WHERE ${accept_filter}`);

    // Incrementally update the target table.
    let table = publish(target_table, {
        type: "incremental",
        dependencies: [`init_${target_table}`],
        uniqueKey: unique_keys,
    });

    if (batch_size >= 0) {
        table.preOps((ctx) => `
        ${ctx.when(ctx.incremental(), `REPEAT --;`)}`);
    }
    table.query((ctx) => `
        SELECT * FROM ${ml_function} (
            MODEL ${ctx.resolve(ml_model)},
            (SELECT S.* FROM (${source_func(ctx)}) AS S
                ${ctx.when(ctx.incremental(), 
                `WHERE NOT EXISTS (SELECT * FROM ${ctx.resolve(target_table)} AS T WHERE ${unique_keys.map((k) => `S.${k} = T.${k}`).join(' AND ')})`)} ${limit_clause}),
            STRUCT (${ml_configs_string})
        ) WHERE ${accept_filter}`);
    if (batch_size >= 0) {
        table.postOps((ctx) => `${ctx.when(ctx.incremental(), 
        `UNTIL (SELECT @@row_count) = 0 OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), @@script.creation_time, SECOND) >= ${batch_duration_secs}
         END REPEAT`, ``)}`);
    }
};

const generate_embedding = (source_table, target_table, unique_keys, ml_model, source_query, ml_configs, options) => {
    common.declare_resolvable(source_table);
    common.declare_resolvable(ml_model);

    table_ml(target_table, unique_keys, "ML.GENERATE_EMBEDDING", ml_model, source_query,
        common.retryable_error_filter("ml_generate_embedding_status"), ml_configs, options);
};

const generate_text = (source_table, target_table, unique_keys, ml_model, source_query, ml_configs, options) => {
    common.declare_resolvable(source_table);
    common.declare_resolvable(ml_model);

    table_ml(target_table, unique_keys, "ML.GENERATE_TEXT", ml_model, source_query,
        common.retryable_error_filter("ml_generate_text_status"), ml_configs, options);    
}

const understand_text = (source_table, target_table, unique_keys, ml_model, source_query, ml_configs, options) => {
    common.declare_resolvable(source_table);
    common.declare_resolvable(ml_model);

    table_ml(target_table, unique_keys, "ML.UNDERSTAND_TEXT", ml_model, source_query,
        common.retryable_error_filter("ml_understand_text_status"), ml_configs, options);    
}

const translate = (source_table, target_table, unique_keys, ml_model, source_query, ml_configs, options) => {
    common.declare_resolvable(source_table);
    common.declare_resolvable(ml_model);

    table_ml(target_table, unique_keys, "ML.TRANSLATE", ml_model, source_query,
        common.retryable_error_filter("ml_translate_status"), ml_configs, options);    
}

module.exports = {
    table_ml: table_ml,
    generate_embedding: generate_embedding,
    generate_text: generate_text,
    understand_text: understand_text,
    translate: translate,
}
