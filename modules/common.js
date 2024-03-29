module.exports = {
    /**
     * Declare the given source a resolvable Dataform data source. 
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

    retryable_error_filter: (status_col) => {
        return `${status_col} NOT LIKE 'A retryable error occurred:%'`;
    },
};
