import logging
logger = logging.getLogger(__name__)

def try_restart(fn_restart):
    import os.path
    import hoomd_script as hoomd
    try:
        if os.path.isfile(fn_restart):
            hoomd.init.reset()
            system = hoomd.init.read_xml(filename = fn_restart)
            return system
        else:
            logger.debug("Could not find any restart file with name '{}'.".format(fn_restart))
            raise RuntimeError()
    except RuntimeError:
        raise FileNotFoundError()

def make_write_restart_file_callback(fn_restart):
    fn_tmp = fn_restart + '.tmp'
    def callback(step):
        import hoomd_script as hoomd
        import os
        if hoomd.comm.get_rank() == 0:
            logger.info("Saving restart file {} at step {}.".format(fn_restart, step))
        hoomd.dump.xml(fn_tmp, vis = True, orientation = True)
        if hoomd.comm.get_rank() == 0:
            try:
                os.rename(fn_tmp, fn_restart)
            except Exception as error:
                logger.error(error)
                raise
            else: logger.debug("Renamed '{}' -> '{}'.".format(fn_tmp, fn_restart))
    return callback
