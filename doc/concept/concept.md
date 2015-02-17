# Computational Materials Database

## About

This document describes the concept and the proposed development process for a computational materials database.
The database serves as the primary hub to import and export simulation data of materials simulations.

For early tests the database is exclusively used by the Glotzer Group at the University of Michigan.
Later stages may provide (general) public access.

## Development Process

### Development Stages

  1. Basic Design / Requirements
  
    a) Concept for technical framework
    b) Concept for standard models
    c) Test of alpha features
    
  2. Detailed Design / Early Implementation

    a) Revision of technical framework
    b) Revision of standard models

  3. Implementation
  4. Testing

### Milestones
  
  1. Define technical framework.
  2. Define internal standard for model representation.
  3. Provide testing data.
  4. Implement alpha requirements.
    * Import structures from local files
    * Export structures to local files
    * Import structures for simulation runs
    * Export structures from simulation runs
  5. Implement beta requirements.
    * View files in browser
    * Import and export force field setup
    * Import and export job script
    * Generate pair potential from DB

## Technical Description

## Use cases

  * Import structures from simulation data
  * Import structures from other databases
  * Export structures for analysis
  * Export structures for simulation
  * View structures online
  * View analysis data online
  * Apply analytical methods on database entries directly

## Features
    
  * All structures associated with citation info.
  * Structures can be associated with simulation data.
  * Structures can be associated with simulation job descriptions
  * Auto-fetch of results, where results are available.

## Supported Formats

It is possible to store structures in any arbitrary format.
The framework knows how to interpret or translate between diffrent classified formats.

Classified for import:

  * HoomdXML
  * CIF
  * libgtar
  * PDB

Classified for export:

  * HoomdXML
  * CIF
  * numpy arrays

## Supporting tools

  * django
  * MongoDB
  * JMol
  * injavis
