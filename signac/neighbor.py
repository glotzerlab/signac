from functools import partial
from collections import defaultdict

from .job import calc_id
from ._utility import _to_hashable, _dotted_dict_to_nested_dicts, _nested_dicts_to_dotted_keys
from ._search_indexer import _DictPlaceholder

def prepare_shadow_project(sp_cache, ignore: list):
    """Build cache and mapping for shadow project, which comes from ignored keys.

    We use cache lookups for speedy searching. Ignoring a key creates a subset of jobs, now
    identified with different job ids. Call it "shadow" job id because we're making a projection of
    the project.

    We can map from the shadow job id to the actual job id in the use cases identified.
    Raise ValueError if this mapping is ill defined.

    We can detect the neighbor list on the shadow project then map it back
    to the real project.

    Parameters
    ----------
    sp_cache, state point cache
    ignore: list of str
        state point keys

    Returns
    -------
    shadow_map is a map from shadow job id to project job id.

    shadow_cache is an in-memory state point cache for the shadow project
    mapping shadow job id --> shadow state point


    Use cases:

    1) Seed that is different for every job.

    2) State point key that changes in sync with another key.

    Case 1:

    {"a": 1, "b": 2, "seed": 0} -> jobid1
    {"a": 1, "b": 3, "seed": 1} -> jobid2
    {"a": 1, "b": 2} -> shadowid1
    {"a": 1, "b": 3} -> shadowid2

    shadowid1 <---> jobid1
    shadowid2 <---> jobid2

    Breaking case 1 with repeated shadow jobs:

    {"a": 1, "b": 2, "seed": 0} -> jobid1
    {"a": 1, "b": 3, "seed": 1} -> jobid2
    {"a": 1, "b": 3, "seed": 2} -> jobid3

    {"a": 1, "b": 2} -> shadowid1
    {"a": 1, "b": 3} -> shadowid2
    {"a": 1, "b": 3} -> shadowid2 *conflict* No longer bijection.
    Now we have shadowid2 .---> jobid2
                          \\--> jobid3

    Case 2:

    {"a1": 10, "a2": 20} -> jobid1
    {"a1": 2, "a2": 4} -> jobid2

    {"a1": 10} -> shadowid1
    {"a1": 2} -> shadowid2

    Can still make the mapping between ids.

    Breaking case 2:
    {"a1": 10, "a2": 20} -> jobid1
    {"a1": 2, "a2": 4} -> jobid2
    {"a1": 2, "a2": 5} -> jobid3

    {"a1": 10} -> shadowid1
    {"a1": 2} -> shadowid2
    {"a1": 2} -> shadowid2 --
    Now we have shadowid2 .---> jobid2
                          \\--> jobid3

    """
    shadow_cache = {} # like a state point cache, but for the shadow project
    job_projection = {} # goes from job id to shadow id
    for jobid, sp in sp_cache.items():
        shadow_sp = dict(sp)
        for ig in ignore:
            shadow_sp.pop(ig, None)
        shadow_id = calc_id(shadow_sp)
        shadow_cache[shadow_id] = shadow_sp
        job_projection[jobid] = shadow_id

    if len(set(job_projection.values())) != len(job_projection):
        # Make a helpful error message for map that has duplicates
        shadow_to_job = defaultdict(list)
        counts = defaultdict(int)
        for job_id, shadow_id in job_projection.items():
            shadow_to_job[shadow_id].append(job_id)
            counts[shadow_id] += 1
        bad_jobids = [shadow_to_job[shadow_id] for shadow_id, num in counts.items() if num > 1]
        err_str = "\n".join(f"Job ids: {', '.join(j)}." for j in bad_jobids)
        raise ValueError(f"Ignoring {ignore} makes it impossible to distinguish some jobs:\n{err_str}")
    # map from shadow job id to project job id
    shadow_map = {v: k for k, v in job_projection.items()}
    return shadow_map, shadow_cache

# key and other_val provided separately to be used with functools.partial
def _search_cache_for_val(statepoint, cache, key, other_val):
    """Return job id of a job similar to statepoint if present in cache.

    The similar job is obtained by modifying statepoint to include {key: other_val}.

    Internally converts statepoint from dotted keys to nested dicts format.

    Parameters
    ----------
    statepoint : dict
        state point of job to modify. statepoint must not be a reference because it will be
        modified in this function
    cache : dict
        project state point cache to search in
    key : str
        The key whose value to change
    other_val
        The new value of key to search for

    Returns
    -------
    job id of similar job
    None, if not present
    """
    statepoint.update({key: other_val})
    # schema output not compatible with dotted key notation
    statepoint = _dotted_dict_to_nested_dicts(statepoint)
    other_job_id = calc_id(statepoint)
    if other_job_id in cache:
        return other_job_id
    else:
        return None

def _search_out(search_direction, values, current_index, boundary_index, search_fun):
    """Search in values towards boundary_index from current_index using search_fun.

    Parameters
    ----------
    search_direction : int, 1 or -1
        1 means search in the positive direction from the index
    values : iterable
        values to index into when searching
    current_index : int
       index into values to start searching from.
       The value at this index is not accessed directly.
    boundary_index : int
        the index at which to stop
    search_fun : function
        unary function returning jobid if it exists and None otherwise

    Returns
    -------
    None if jobid not found
    
    {val: jobid} if jobid found per search_fun
    jobid : str
        job id of the nearest job in the search_direction
    val : value of the key at the neighbor jobid
    """
    query_index = current_index + search_direction
    # search either query_index >= low_boundary or query_index <= high_boundary
    while search_direction * query_index <= boundary_index * search_direction:
        val = values[query_index]
        jobid = search_fun(val)
        if jobid is None:
            query_index += search_direction
        else:
            return {val: jobid}
    return None

def neighbors_of_sp(statepoint, dotted_sp_cache, sorted_schema):
    """Return neighbors of given state point by searching along sorted_schema in dotted_sp_cache.

    State point and cache must both use either job ids or shadow job ids.

    dotted_sp_cache must be in dotted key format, which is accessed by calling
    _nested_dicts_to_dotted_keys on each state point in the cache.

    Parameters
    ----------
    statepoint : dict
        Place to search from
    dotted_sp_cache : dict
        Map from job id to state point in dotted key format
    sorted_schema : dict
        Map from key (in dotted notation) to sorted values of the key to search over
    """

    neighbors = {}
    for key, schema_values in sorted_schema.items(): # from project
        # allow comparison with output of schema, which is hashable
        value = _to_hashable(statepoint.get(key, _DictPlaceholder))
        if value is _DictPlaceholder:
            # Possible if schema is heterogeneous
            continue
        value_index = schema_values.index(value)
        # need to pass statepoint by copy
        search_fun = partial(_search_cache_for_val, dict(statepoint), dotted_sp_cache, key)
        prev_neighbor = _search_out(-1, schema_values, value_index, 0, search_fun)
        next_neighbor = _search_out(1, schema_values, value_index, len(schema_values) - 1, search_fun)

        this_d = {}
        if next_neighbor is not None:
            this_d.update(next_neighbor)
        if prev_neighbor is not None:
            this_d.update(prev_neighbor)
        neighbors.update({key: this_d})
    return neighbors

def shadow_neighbor_list_to_neighbor_list(shadow_neighbor_list, shadow_map):
    """Replace shadow job ids with actual job ids in the neighbor list.

    Parameters
    ----------
    shadow_neighbor_list : dict
        neighbor list containing shadow job ids
    shadow_map : dict
        map from shadow job id to project job id
    """
    neighbor_list = dict()
    for jobid, neighbors in shadow_neighbor_list.items():
        this_d = {}
        for neighbor_key, neighbor_vals in neighbors.items():
            this_d[neighbor_key] = {k: shadow_map[i] for k,i in neighbor_vals.items()}
        neighbor_list[shadow_map[jobid]] = this_d
    return neighbor_list

def _build_neighbor_list(dotted_sp_cache, sorted_schema):
    """Iterate over cached state points and get neighbors of each state point.

    Parameters
    ----------
    dotted_sp_cache : dict
        Map from job id to state point OR shadow job id to shadow state point in dotted key format
    sorted_schema : dict
        Map of keys to their values to search over

    Returns
    -------
    neighbor_list : dict
        {jobid: {state_point_key: {prev_value: neighbor_id, next_value: neighbor_id}}}
    """
    neighbor_list = {}
    for _id, _sp in dotted_sp_cache.items():
        neighbor_list[_id] = neighbors_of_sp(_sp, dotted_sp_cache, sorted_schema)
    return neighbor_list

def get_neighbor_list(sp_cache, sorted_schema, ignore):
    """Build neighbor list while handling ignored keys.

    Parameters
    ----------
    sp_cache : dict
        Project state point cache
    sorted_schema : dict
        Map of keys to their values to search over

    Returns
    -------
    neighbor_list : dict
        {jobid: {state_point_key: {prev_value: neighbor_id, next_value: neighbor_id}}}
    """
    if len(ignore) > 0:
        shadow_map, shadow_cache = prepare_shadow_project(sp_cache, ignore = ignore)
        nl = _build_neighbor_list(shadow_cache, sorted_schema)
        return shadow_neighbor_list_to_neighbor_list(nl, shadow_map)
    else:
        # the state point cache is incompatible with nested key notation
        for _id, _sp in sp_cache.items():
            sp_cache[_id] = {k : v for k, v in _nested_dicts_to_dotted_keys(_sp)}
        return _build_neighbor_list(sp_cache, sorted_schema)
