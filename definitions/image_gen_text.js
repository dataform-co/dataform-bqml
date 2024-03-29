const bqml = require("../index");

let model = "multi-llm";
let obj_table = "imagesets";
let target_table = "imagesets_description";

bqml.vision_generate_text(
    obj_table, target_table, model, 
    "Describe the image in less than 20 words", {
        flatten_json_output: true
    }
);
