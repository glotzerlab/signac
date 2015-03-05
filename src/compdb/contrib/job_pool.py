class Pool(object):

    def __init__(self, project, parameter_set, exclude_condition = None):
        self._project = project
        self._parameter_set = parameter_set
        self._exclude_condition = exclude_condition

    def get_id(self):
        from . hashing import generate_hash_from_spec
        pool_spec = {
            'project_id': self._project.get_id(),
            'set':  self._parameter_set}
        return generate_hash_from_spec(pool_spec)

    def _fn_pool(self):
        from os.path import join
        return join(
            self._project._workspace_dir(), 
            '_job_pool_{}'.format(self.get_id()))

    def _fn_pool_counter(self):
        from os.path import join
        return join(
            self._project._workspace_dir(), 
            '_job_pool_counter_{}'.format(self.get_id()))

    def _get_valid_indeces(self):
        if self._exclude_condition is None:
            yield from range(len(self._parameter_set))
        else:
            docs = list(self._project.find(spec = self._exclude_condition))
            doc_ids = set(doc['_id'] for doc in docs)
            job_ids = (self._project.open_job('', p).get_id() for p in self._parameter_set)
            for index, job_id in enumerate(job_ids):
                if job_id in doc_ids:
                    continue
                else:
                    yield index

    def __len__(self):
        return len(list(self._get_valid_indeces()))

    def _update_counter(self, delta):
        import fcntl
        fn_pool_counter = self._fn_pool_counter()
        try:
            with open(fn_pool_counter, 'xb') as file:
                file.write(str(1).encode())
        except FileExistsError:
            with open(fn_pool_counter, 'r+b') as file:
                fcntl.flock(file, fcntl.LOCK_EX)
                counter = int(file.read().decode())
                file.seek(0)
                file.truncate()
                file.write(str(counter+delta).encode())

    def setup(self):
        fn_pool = self._fn_pool()
        try:
            indeces = self._get_valid_indeces()
            str_indeces= ','.join(str(i) for i in indeces)
            with open(fn_pool, 'xb') as file:
                file.write(str_indeces.encode())
        except FileExistsError:
            pass

    def __enter__(self):
        self.setup()
        self._update_counter(1)
        return self

    def __exit__(self, err_type, err_val, traceback):
        import fcntl
        self._update_counter(-1)

    def _get_index(self, rank):
        with open(self._fn_pool(), 'rb') as file:
            indeces = file.read().decode().split(',')
            try:
                return int(indeces[rank])
            except ValueError:
                raise IndexError(rank)

    def parameters(self, rank):
        return self._parameter_set[self._get_index(rank)]

    def open_job(self, jobname, rank):
        return self._project.open_job(jobname, self.parameters(rank))
