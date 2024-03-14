const bqml = require("../index");

let model = "vision";
let obj_table = "imagesets";
let target_table = "annotated_imagesets";

bqml.annotate_image(obj_table, target_table, model, ['LABEL_DETECTION']);
