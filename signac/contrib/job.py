import os
import errno
import logging
import json

from ..core.jsondict import JSonDict
from .hashing import calc_id

logger = logging.getLogger(__name__)


def mkdir_p_(path):
    try:
        os.makedirs(path)
    except OSError as error:
        if not (error.errno == errno.EEXIST and os.path.isdir(path)):
            raise


class Job(object):
    FN_MANIFEST = 'signac_statepoint.json'

    def __init__(self, project, parameters):
        self._project = project
        self._parameters = parameters
        self._id = calc_id(parameters)
        self._document = None
        self._wd = os.path.join(self._project.config[
                                'workspace_dir'], str(self))
        self._cwd = None

    def get_id(self):
        return self._id

    def __str__(self):
        "Returns the job's id."
        return str(self.get_id())

    def workspace(self):
        return self._wd

    def parameters(self):
        return dict(self._parameters)

    @property
    def document(self):
        return self._get_document()

    def _get_document(self):
        if self._document is None:
            self._create_directory()
            fn = os.path.join(self.workspace(), 'signac_job_document.json')
            self._document = JSonDict(
                fn, synchronized=True, write_concern=True)
        return self._document

    def _create_directory(self):
        mkdir_p_(self.workspace())
        with open(os.path.join(self.workspace(),
                               self.FN_MANIFEST), 'w') as file:
            file.write(json.dumps(self.parameters()))

    def open(self):
        # The following code is not tolerant against race conditions!
        if self._cwd is None:
            self._cwd = os.getcwd()
            self._create_directory()
            logger.info("Enter workspace '{}'.".format(self.workspace()))
            os.chdir(self.workspace())
        else:
            logger.debug("Job is already opened, doing nothing.")

    def close(self):
        logger.info("Leave workspace.")
        os.chdir(self._cwd)
        self._cwd = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, err_type, err_value, tb):
        self.close()
        return False
