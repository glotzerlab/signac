def minimal_find_structure_example():
    import signac
    a_structure = signac.db.find_one('tetrahedron')
    a_structure.write('_structure_file.xml')

def minimal_store_structures_example():
    import signac
    
    ## Some code to define a structure
    my_structure = signac.parse_structure(
        filename = '_structure_file.xml', 
        fileformat = 'hoomd_blue_xml')
    signac.db.store(
        structure = my_structure,
        author = signac.get_author('jondoe'),
        project = 'example_project')

def complete_find_and_store_structures_example():
    # How to find structures from the database
    import signac

    # This will give you all tetrahedron structures in the database
    all_tetrahedrons = signac.db.find_structure(name = 'tetrahedron')

    # Use filters and order arguments to get a more specified result.
    tetrahedrons = signac.db.find_structure(name = 'tetrahedron'
        filter = {'project_id': 'example_project'},
        order_by = {'uploaded': - 1})

    # Chose the latest uploaded version, which is part of this project
    tetrahedron = tehtrahedrons[0]

    # Export the structure into the working directory
    tetrahedron.write('_tmp_structure.xml', fileformat = 'hoomd_blue_xml')
    init_xml('_tmp_structure.xml')        # Initialize a hoomd simulation from this file

    # Writing into a spcific fileformat is only possible,
    # if the structure is in a native format.
    # This means `signac` knows how to parse and write this format.
    # It is always possible to write the file in its original format.

    # You can also use an external database to find structures
    protein = signac.db.find_structure({'name': '2MQS'}, source = 'PDB')    # Protein database
    crystal = signac.db.find_structure({'name': 'as34234'}, source = 'CSD') # Cambridge Structural Database

    # How to export structures to the database
    #

    # From file with native fileformat
    result_structure = signac.parse_structure(
        filename = '_my_structure.xml',
        fileformat = 'hoomd_blue_xml')

    # From file without native fileformat
    result_structure = signac.read_structure('_my_structure.pos')

    # Specific structures
    molecule_structure = signac.read_structure('_my_structure.pos')
    molecule = signac.MoleculeStructure(
        result_molecule,
        IUPAC_name = '7-(Phenylsulfonyl)quinoline')

    export_id = signac.export_structure(
        structure = molecule,
        author = signac.get_author('jondoe'),
        project = 'example_project')

def special_methods_hoomd():
    signac.utils.hoomd.init(tetrahedron)     # Or init directly from the structure instance
