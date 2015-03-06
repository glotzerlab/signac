import logging
logger = logging.getLogger('job_pool')

class JobPool(object):

    def __init__(self, project, parameter_set, exclude_condition = None):
        self._open = False
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
            job_ids = (self._project.open_job(p).get_id() for p in self._parameter_set)
            for index, job_id in enumerate(job_ids):
                if job_id in doc_ids:
                    continue
                else:
                    yield index

    def __len__(self):
        return len(list(self._get_valid_indeces()))

    def _update_counter(self, delta):
        import fcntl, os
        fn_pool_counter = self._fn_pool_counter()
        try:
            with open(fn_pool_counter, 'xb') as file:
                fcntl.flock(file, fcntl.LOCK_EX)
                file.write(str(delta).encode())
                self._setup()
        except FileExistsError:
            with open(fn_pool_counter, 'r+b') as file:
                fcntl.flock(file, fcntl.LOCK_EX)
                counter = int(file.read().decode())
                counter += delta
                if counter == 1:
                    try:
                        self._setup()
                    except FileExistsError:
                        pass
                elif counter == 0:
                    os.remove(self._fn_pool())
                elif counter < 0:
                    return
                file.seek(0)
                file.truncate()
                file.write(str(counter).encode())

    def _setup(self):
        fn_pool = self._fn_pool()
        #try:
        indeces = self._get_valid_indeces()
        str_indeces= ','.join(str(i) for i in indeces)
        with open(fn_pool, 'xb') as file:
            file.write(str_indeces.encode())
        #except FileExistsError:
            #pass

    def open(self):
        if not self._open:
            self._update_counter(1)
            self._open = True

    def close(self):
        if self._open:
            self._update_counter(-1)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, err_type, err_val, traceback):
        self.close()

    def _get_index(self, rank):
        try:
            with open(self._fn_pool(), 'rb') as file:
                indeces = file.read().decode().split(',')
                try:
                    return int(indeces[rank])
                except (ValueError, IndexError) as error:
                    msg = "Invalid rank: {}"
                    logger.error(msg.format(rank))
                    raise IndexError(msg.format(rank)) from error
        except FileNotFoundError:
            msg = "Pool not opened."
            raise RuntimeError(msg)

    def parameters(self, rank):
        return self._parameter_set[self._get_index(rank)]

    def open_job(self, rank):
        return self._project.open_job(self.parameters(rank))
