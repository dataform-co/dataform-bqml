module.exports = {
    /**
     * Declares the resolvable as a Dataform data source.
     */
    declare_resolvable: (source) => {
        if (source.constructor === Object) {
            declare(source);
        } else {
            declare({
                name: source
            });
        }
    },

    /**
     * Forms a SQL filter clause for filtering out retryable 
     * error based on a given status column.
     */
    retryable_error_filter: (status_col) => {
        return `${status_col} NOT LIKE 'A retryable error occurred:%'`;
    },
};
