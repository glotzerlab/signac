from .template_example import EXAMPLE
from .template_minimal import MINIMAL
from .template_testing import TESTING

TEMPLATES = {
    'minimal': MINIMAL,
    'example': EXAMPLE,
    'testing': TESTING,
}

RESTORE_SH = """PROJECT={project}
DATABASE_HOST={db_host}
FILESTORAGE_DIR={fs_dir}
DATABASE_META={db_meta}
JOBS_COLLECTION={signac_docs}
DOCS_COLLECTION={signac_job_docs}

mongoimport --host ${{DATABASE_HOST}} -db ${{DATABASE_META}} --collection ${{JOBS_COLLECTION}} signac_jobs.json
mongoimport --host ${{DATABASE_HOST}} -db ${{PROJECT}} --collection ${{DOCS_COLLECTION}} signac_docs.json
if [ -d "{sn_storage_dir}" ]; then
    mv {sn_storage_dir}/* ${{FILESTORAGE_DIR}}
fi"""
