const bqml = require("../index");

let source_table = "hacker_50k";
let target_table = "hacker_50k_gentext";
let keys = ["id"];
let model = "llm";

bqml.generate_text(
    source_table,
    target_table,
    keys,
    model,
    (ctx) => `SELECT *, CONCAT("Summarize the following content: ", text) AS prompt FROM ${ctx.resolve(source_table)}`,
    {flatten_json_output: true});