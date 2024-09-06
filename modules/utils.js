/**
 * Declares the resolvable as a Dataform data source.
 */
function declare_resolvable(resolvable) {
    return declare(resolvable.constructor === Object ? resolvable :{ name : resolvable});
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
};
