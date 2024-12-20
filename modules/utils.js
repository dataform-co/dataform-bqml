/**
 * Convert passed parameter to Resolvable
 * @param {String | Object} resolvable a resolvable can be either the name of an entity as a string, or
 * an object that describes the full path to the relation.
 */
function to_resolvable(resolvable) {
    return resolvable.constructor === Object ? resolvable :{ name : resolvable};
}

function resolvable(name, default_resolvable) {
    return { ...default_resolvable, name };
}

/**
 * Declares the resolvable as a Dataform data source.
 */
function declare_resolvable(resolvable) {
    const convertedResolvable = to_resolvable(resolvable);
    return declare(convertedResolvable);
}


/**
 * Forms a SQL filter clause for filtering out retryable
 * error based on a given status column.
 */
function retryable_error_filter(status_col) {
    return `${status_col} NOT LIKE 'A retryable error occurred:%'`;
}

module.exports = {
    declare_resolvable,
    retryable_error_filter,
    to_resolvable,
    resolvable,
};
