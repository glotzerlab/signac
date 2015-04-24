import logging
logger = logging.getLogger('compdb.milestones')

MILESTONE_KEY = 'milestones'

class Milestones(object):

    def __init__(self, project, job_id):
        self._project = project
        self._job_id = job_id

    def _spec(self):
        return {'_id': self._job_id}

    def _collection(self):
        return self._project.get_jobs_collection()

    def mark(self, name):
        result = self._collection().update(
            self._spec(),
            {'$addToSet': {MILESTONE_KEY: name}},
            upsert = True)
        assert result['ok']

    def remove(self, name):
        assert self._collection().update(
            self._spec(),
            {'$pull': {MILESTONE_KEY: name}})['ok']

    def reached(self, name):
        spec = self._spec()
        spec.update({
            MILESTONE_KEY: { '$in': [name]}})
        result = self._collection().find_one(spec, [MILESTONE_KEY])
        logger.debug(result)
        return result is not None

    def __contains__(self, name):
        return self.reached(name)

    def clear(self):
        self._collection().update(
            self._spec(),
            {'$unset': {MILESTONE_KEY: ''}})

