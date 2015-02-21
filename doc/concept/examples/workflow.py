def minimal_job_example():
    # The end-users' workflow should be as undisturbed as possible,
    # which is why the bare minimum example to execute code in the
    # context of the compdb does not require any additional boiler-code,
    # but the enclosing manager.
    import compdb
    with compdb.contrib.open_job('example_job') as job:
        job.restore_files()
        # Execution code here.
        job.store_files()

def job_concurrency_example():
    import compdb
    with compdb.contrib.open_job('example_job') as job:
        
        # Check for multiple execution
        if job.is_running():
            return

        # Try to acquire a job lock, wait for 2 hours, if necessary.
        with job.lock(blocking = True, timeout = 3600 * 2):
            # This prevents the execution of the protected code
            # in parallel.
        except TimeoutError as error:
            print("Timed out while trying to acquire lock.")
            raise

def complete_job_execution_example():
    for params in parameter_set:
        with compdb.contrib.open_job(
            name = 'example_job',
            project = 'example_project',
            author = compdb.get_author('johndoe'),
            parameters = params) as job:
            # Some of the arguments should be more conventiently provided by 
            #   *) environment variables, 
            #   *) a config file in the working or project directory.
            # The script will automatically change to the working directory.

            # Although it is possible to execute the same job multiple times in parallel,
            # that might not be desired. In this case we should skip the job execution.
            if job.is_running():
                continue

            # The same goes for already completed jobs.
            # We check this by looking up previous results.
            if job.find_one('simulation_end') is not None:
                continue
            # job.find_one is a wrapper for:
            # job.collection.find_one({'simulation_end': {'$exists': True})


            # Enter the working directory
            job.enter_working_dir()

            # Print the job metadate to the log file.
            print(job)

            ##
            ## INSERT YOUR SIMULATION EXECUTION CODE HERE
            ##

            # How to store results.
            #

            # Access the job related MongoDB collection
            import datetime
            job.collection.insert({
                'simulation_start': datetime.now()})
            # job.insert({'simulation_start', datetime.now()}) # Short-cut for the above code.

            # Store data related to this job
            with open('_my_data.tar.gz', 'rb') as file:
                job.store_binary('my_data', file.read().encode())

            # Example on how to store multiple files in your working directory.
            job.store_files('_*')
            # This is essentially equivalent to
            import glob
            for filename in glob.glob('_*'): # ex. for all files that start with underscore
                job.store_file(filename) # wrapper for store_binary
            # If you want to store or files just
            job.store_files()

            # Example on how to store natively supported data.
            # 
            snapshot = compdb.data.Snapshot(filename = '_dump.xml', fileformat = 'hoomd_blue_xml')
            job.store('snapshot', snapshot)
            job.store('hoomd_snapshot', compdb.hoomd.snapshot(vis = True)) 

            # Storing very large datafiles might result in extensive storage loads.
            # If we do not need a copy in the working directory anymore we should move the file instead.
            trajectory = compdb.data.Trajectory('_dump.xml', '_dump.dcd')
            job.store('trajectory', trajectory, move = True) 

            # Job completed
            job.store({'simulation_end', datetime.now()})
            job.clean() # Delete all files in working directory.

def complete_job_post_processing_example():
    for params in parameter_set:
        with compdb.contrib.open_job(
            name = 'example_job',
            project = 'example_project',
            author = compdb.get_author('johndoe'),
            parameters = params) as job:

            # Do not execute if job is running, post_processing or already executed or no
            # simulation data available.
            if job.running() or job.find_one('pp_data') or not job.find_one('simulation_end'):
                continue
            
            # Access previously stored native data
            trajectory = job.find_one('trajectory') # for specific data or
            trajectory.write('_dump.dcd')

            # Binary data
            my_data = job.find_one('my_data')
            with open('_my_data.tar.gz', 'wb') as file:
                file.write(my_data)

            # Or you could restore your working directory
            job.restore_all()


            ##
            ## INSERT ANALYSIS CODE HERE
            ##

            with open('_pp_analysis_data.tar.gz', 'rb') as file:
                job.store_binary('pp_data', file.read())

def complete_job_post_processing_as_own_job_example():
    for params in parameter_set:
        with compdb.contrib.open_job(
            name = 'pp_example_job',
            project = 'example_project',
            author = compdb.get_author('johndoe'),
            parameters = params) as pp_job:
            with compdb.contrib.open_job(
                name = 'example_job',
                project = 'example_project',
                author = compdb.get_author('johndoe'),
                parameters = params,
                read_only = True) as sim_job:
                    my_data = job.find_one('my_data')

                    # [POSTPROCESSING]

                    pp_job.store('pp_data', pp_data.encode())

def low_level_access():
    # Sometimes you want to have more control over your workflow,
    # which is why the following resources are available.
    with compdb.contrib.open_job(
        name = 'example_job',
        project = 'example_project',
        author = compdb.get_author('johndoe'),
        parameters = params) as job:
        
        # The unique job id
        _id = job.get_id()

        # The project database
        db = compdb.job.get_project_db()

        # The project directory
        project_dir = compdb.job.get_project_dir()

        # The filestorage directory
        filestorage = compdb.job.get_filestorage_dir()

        # The working directory
        job_wd = job.get_working_dir() # Usually equal to os.cwd()


def config():
    import compdb

    # Upon import the package tries to determine your configuration.
    #
    # Settings are determined in the following order:
    #
    #   1) from command line arguments,
    #   2) from enviroment variables,
    #   3) from a configuration file in
    #       a) the working directory,
    #       b) the project directory,
    #       c) the home directory,
    
    # All of this information is accessable.

    # Any of the following functions will raise an exception, 
    # if it could not be determined.
    author = compdb.get_author() # Your author information.
    project = compdb.get_project() # Project meta data.

    # These functions are accessed, when opening a job, which
    # is why an exception will be raised, if they are not 
    # available.

def import_and_export_structures_example():
    # How to import structures from the database
    import compdb

    # This will give you all tetrahedron structures in the database
    all_tetrahedrons = compdb.import_structure(name = 'tetrahedron')

    # Use filters and order arguments to get a more specified result.
    tetrahedrons = compdb.import_structure(name = 'tetrahedron'
        filter = {'project_id': 'example_project'},
        order_by = {'uploaded': - 1})

    # Chose the latest uploaded version, which is part of this project
    tetrahedron = tehtrahedrons[0]

    # Export the structure into the working directory
    tetrahedron.write('_tmp_structure.xml', fileformat = 'hoomd_blue_xml')

    init_xml('_tmp_structure.xml')        # Initialize a hoomd simulation from this file
    compdb.hoomd.init(tetrahedron)     # Or init directly from the structure instance

    # Writing into a spcific fileformat is only possible,
    # if the structure is in a native format.
    # This means `compdb` knows how to parse and write this format.
    # It is always possible to write the file in its original format.

    # You can also use an external database to import structures
    protein = compdb.import_structure('2MQS', source = 'PDB')    # Protein database
    crystal = compdb.import_structure('as34234', source = 'CSD') # Cambridge Structural Database

    # How to export structures to the database
    #

    # From file with native fileformat
    result_structure = compdb.parse(
        filename = '_my_structure.xml',
        fileformat = 'hoomd_blue_xml')

    # From file without native fileformat
    result_structure = compdb.read_structure('_my_structure.pos')        # Only one of these patterns 
    result_structure = compdb.Structure().parse('_my_structure.pos')     # will survive.

    # Be more specific
    molecule_structure = compdb.read_structure('_my_structure.pos')
    molecule = compdb.MoleculeStructure(
        result_molecule,
        IUPAC_name = '7-(Phenylsulfonyl)quinoline')

    export_id = compdb.export_structure(
        molecule,
        project = 'example_project')
