/**
 * Can resolve both a string name and an object dataform configuration
 * @param {*} source
 */
function declare_resolvable(source) {
    if (source.constructor === Object) {
        declare(source);
    } else {
        declare({
            name: source,
        });
    }
}

function retryable_error_filter(status_col) {
    return `${status_col} NOT LIKE 'A retryable error occurred:%'`;
}

/**
 * Check if a dataform action is already declared
 * @param {*} name
 * @returns
 */
function is_declared(name) {
    return dataform.actions.some((action) => action.proto.target.name === name);
}

/**
 * Delcare a dataform object if it is not already declared
 * @param  {...any} objects
 */
function safe_declare_resolvable(...objects) {
    objects.forEach((obj) => {
        if (!is_declared(obj.name)) {
            declare_resolvable(obj);
        } else {
            console.log(`${obj.name} already declared`);
        }
    });
}

module.exports = {
    declare_resolvable,
    retryable_error_filter,
    is_declared,
    safe_declare_resolvable,
};
