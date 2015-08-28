def standard_methods_minimal_example():
    from signac.methods import StandardMethod
    method = StandardMethod()

    # Standard methods are canonicalized in the collection.
    # This allows us to search for those results.
    # The module checks the version and stores the result 
    # associated with the version.
    
    # Let's assume we have a method, called 'StandardMethod',
    # which we can apply to any kind of polyhedron, and we
    # want to apply it to all tetrahedrons in the database.

    # We first try to find all structures, which are available.
    spec = {
        'class':    'polyehedron',
        'facets':   12
        }
    tetrahedrons = signac.db.find(spec)
    parameters = {'a': 0, 'b': 1}

    # Iterate through the structures and apply the method
    for tetrahedron in tetrahedrons:
        # The database will be searched, if the method has been
        # applied for this structure with the given parameters.
        # If there are no results available, the method will be
        # executed.
        result = method.result(tetrahedron, parameters)

def standard_method_concurrent_example():
    # Using python3 concurrent library to apply methods concurrently
    from signac.methods import StandardMethod
    method = StandardMethod()

    tetrahedrons = signac.db.find({'class': 'polyhedron', 'facets': 12})
    import concrurrent.futures
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # The method's results will either be fetched from the database
        # or the method will actually be executed in each processor.
        futures = {executor.submit(method.result, tetrahedron, {'a': 0, 'b': 1}) for tetrahedron in tetrahedrons}
        for future in concurrent.futures.as_completed(futures):
        try:
            data = future.result()
        except Exception as error:
            print("Error: {}".format(error))
        else:
            print(data)

def standard_expensive_method_example():
    # For very expensive methods, we need to refine our behaviour.
    from signac.methods import StandardMethod
    method = StandardMethod()

    tetrahedrons = signac.db.find({'class': 'polyhedron', 'facets': 12})

    futures = {StandardMethod().result(tetrahedrons, {'a': 0, 'b': 1}) for tetrahedron in tetrahedrons}
    for future in futures:
        # The result method would block until results are available.
        # (Either from this process or any other process.)
        # For expensive jobs, this behaviour is usually not preferred.
        # We can prevent blocking and react with exception handling.
        try:
            result = future.result(blocking = False)
        except signac.methods.MethodInExecutionWarning as warning:
            # We can now either decide to wait or skip this.
            continue

        # If on the other hand, if we are only interested in results,
        # in case they are already available, we use this argument.
        try:
            result = future.result(execute = False, blocking = False):
        except signac.methods.MethodInExecutionWarning as warning:
            pass
        except signac.methods.NoResultsAvailableError as error:
            pass
