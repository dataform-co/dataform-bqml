const bqml = require("../index");

let model = "speech2text";
let obj_table = "audio";
let target_table = "transcript";

bqml.transcribe(obj_table, target_table, model, {
    "language_codes": ["en-US"],
    "model": "telephony",
    "auto_decoding_config": {}
}, {
    bacth_size: 2
});
