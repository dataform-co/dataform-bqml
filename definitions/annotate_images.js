const bqml = require("../index");

let model = "vision";
let obj_table = "imagesets_500k";
let target_table = "df_annotated_imagesets_500k";

bqml.annotate_image(obj_table, target_table, model, ['LABEL_DETECTION']);
